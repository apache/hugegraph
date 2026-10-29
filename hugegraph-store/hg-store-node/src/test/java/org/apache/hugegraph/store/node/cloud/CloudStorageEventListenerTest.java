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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory.LiveSstFile;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.MetadataSnapshot;
import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CloudStorageEventListener}.
 *
 * <p>Focuses on the relative-key computation ({@code toRelativeKey}) and
 * the {@link CloudStorageEventListener#onTableFileCreated} / {@link
 * CloudStorageEventListener#onTableFileDeleted} delegate calls to the
 * active {@link CloudStorageProvider}.
 */
public class CloudStorageEventListenerTest {

    private static final String DATA_ROOT = "/hugegraph-store/storage";

    private CloudStorageEventListener listener;

    @BeforeEach
    public void setUp() {
        listener = CloudStorageTestFactory.newListener(new CapturingProvider(), List.of(DATA_ROOT));
    }

    // -----------------------------------------------------------------------
    // toRelativeKey
    // -----------------------------------------------------------------------

    @Test
    public void toRelativeKey_stripsDataRootPrefix() {
        String filePath = DATA_ROOT + "/hgstore-metadata/000008.sst";
        assertEquals("hgstore-metadata/000008.sst", listener.toRelativeKey(filePath));
    }

    @Test
    public void constructor_rejectsEmptyDataRoots() {
        // Fail fast with a clear config error instead of an opaque IndexOutOfBoundsException later.
        try {
            CloudStorageTestFactory.newListener(new CapturingProvider(), Collections.emptyList());
            fail("Expected IllegalArgumentException for empty data-root list");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("data root"),
                       "Message should mention data root: " + expected.getMessage());
        }
    }

    @Test
    public void constructor_rejectsNullDataRoots() {
        try {
            CloudStorageTestFactory.newListener(new CapturingProvider(), null);
            fail("Expected IllegalArgumentException for null data-root list");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void constructor_rejectsNullProvider() {
        try {
            CloudStorageTestFactory.newListener(null, List.of(DATA_ROOT));
            fail("Expected NullPointerException for null provider");
        } catch (NullPointerException expected) {
            assertTrue(String.valueOf(expected.getMessage()).contains("provider"),
                       "Message should mention provider: " + expected.getMessage());
        }
    }

    @Test
    public void constructor_sweepsOrphanedStagingPinsInEveryRoot() throws Exception {
        Path root0 = Files.createTempDirectory("hgstore-test-sweep0");
        Path root1 = Files.createTempDirectory("hgstore-test-sweep1");
        try {
            Path sst = Files.writeString(root1.resolve("000003.sst"), "sst");
            Path staging0 = Files.createDirectories(
                    root0.resolve(CloudStorageEventListener.STAGING_DIR));
            Path staging1 = Files.createDirectories(
                    root1.resolve(CloudStorageEventListener.STAGING_DIR));
            Path pin0 = Files.writeString(staging0.resolve("000001.sst.upload-1"), "sst");
            Path pin1 = Files.createLink(staging1.resolve("000003.sst.upload-2"), sst);

            CloudStorageTestFactory.newListener(new CapturingProvider(),
                                                List.of(root0.toString(), root1.toString()));

            assertFalse(Files.exists(pin0), "Orphaned pin in the first root must be removed");
            assertFalse(Files.exists(pin1), "Orphaned pin in the second root must be removed");
            assertTrue(Files.exists(sst), "Sweep must not touch the source SST");
        } finally {
            deleteRecursively(root0.toFile());
            deleteRecursively(root1.toFile());
        }
    }

    @Test
    public void toRelativeKey_stripsDataRootPrefixForPartitionDb() {
        String filePath = DATA_ROOT + "/0/000042.sst";
        assertEquals("0/000042.sst", listener.toRelativeKey(filePath));
    }

    @Test
    public void toRelativeKey_fallsBackToStripLeadingSlash_whenNotUnderDataRoot() {
        String filePath = "/some/other/path/000001.sst";
        assertEquals("some/other/path/000001.sst", listener.toRelativeKey(filePath));
    }

    @Test
    public void toRelativeKey_handlesDataRootWithTrailingSlash() {
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(new CapturingProvider(),
                                                    List.of(DATA_ROOT + File.separator));
        assertEquals("hgstore-metadata/000008.sst",
                     l.toRelativeKey(DATA_ROOT + "/hgstore-metadata/000008.sst"));
    }

    @Test
    public void toRelativeKey_appliesStoreScopePrefix() {
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(new CapturingProvider(),
                List.of(DATA_ROOT), true, 0L, null, new CloudSyncTracker(),
                "store-127.0.0.1_8501");
        String filePath = DATA_ROOT + "/0/000042.sst";
        assertEquals("store-127.0.0.1_8501/0/000042.sst", l.toRelativeKey(filePath));
    }

    // -----------------------------------------------------------------------
    // onTableFileCreated / onTableFileDeleted – with stub provider
    // -----------------------------------------------------------------------

    @Test
    public void onTableFileCreated_delegatesToProvider_withRelativeKey() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-created");
        Path dbDir = tmpRoot.resolve("hgstore-metadata");
        Files.createDirectories(dbDir);
        Path sst = dbDir.resolve("000008.sst");
        Files.write(sst, "sst".getBytes());

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()));

        try {
            l.onTableFileCreated("hgstore-metadata", "default", sst.toString(), 512L);

            waitForCondition(() -> provider.uploads.size() == 1,
                             "expected exactly one async upload");

            assertEquals(1, provider.uploads.size());
            assertTrue(provider.uploads.get(0)[0].contains("/.cloud-upload-staging/"));
            assertEquals("hgstore-metadata/000008.sst", provider.uploads.get(0)[1]);
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileCreated_delegatesToProvider_withStoreScopePrefix() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-scoped");
        Path dbDir = tmpRoot.resolve("0");
        Files.createDirectories(dbDir);
        Path sst = dbDir.resolve("000008.sst");
        Files.write(sst, "sst".getBytes());

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, new CloudSyncTracker(),
                "store-127.0.0.1_8501");
        try {
            l.onTableFileCreated("0", "default", sst.toString(), 512L);

            waitForCondition(() -> provider.uploads.size() == 1,
                             "expected exactly one async upload with store scope prefix");

            assertEquals(1, provider.uploads.size());
            assertEquals("store-127.0.0.1_8501/0/000008.sst", provider.uploads.get(0)[1]);
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileCreated_resolvesPathFormDbName_beforeMetadataSync() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-resolve-db-name");
        Path dbDir = tmpRoot.resolve("00001");
        Files.createDirectories(dbDir);
        Path sst = dbDir.resolve("000012.sst");
        Files.write(sst, "sst".getBytes());

        CountDownLatch metadataSyncCalled = new CountDownLatch(1);
        AtomicReference<String> observedDbName = new AtomicReference<>();
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                new CloudStorageEventListener(provider, List.of(tmpRoot.toString()),
                true, CloudStorageEventListener.DEFAULT_READ_MISS_GUARD_WINDOW_MS, null,
                new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            void markMetadataDirty(String dbName) {
                observedDbName.set(dbName);
                metadataSyncCalled.countDown();
            }
        };

        try {
            // Seed the dir->logical mapping as happens when the DB opens.
            l.onDBCreated("00001", dbDir.toString());

            l.onTableFileCreated(dbDir.toString(), "default", sst.toString(), Files.size(sst));

            assertTrue(metadataSyncCalled.await(2, TimeUnit.SECONDS),
                       "Expected async metadata-sync trigger after upload");
            assertEquals("00001", observedDbName.get());
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileDeleted_resolvesPathFormDbName_beforeGuardAndSync() throws Exception {
        // RocksDB delivers the DB *directory path* (db.getName()) to the delete callback, exactly
        // as it does to the create callback. The listener must resolve it to the logical DB name
        // before the delete guard / metadata sync run — otherwise those key off a path absent from
        // dbSessionMap and every superseded-SST removal is silently skipped (object leaks in cloud).
        Path tmpRoot = Files.createTempDirectory("hgstore-test-resolve-del-db-name");
        Path dbDir = tmpRoot.resolve("00001");
        Files.createDirectories(dbDir);

        AtomicReference<String> observedDbName = new AtomicReference<>();
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                new CloudStorageEventListener(provider, List.of(tmpRoot.toString()),
                true, CloudStorageEventListener.DEFAULT_READ_MISS_GUARD_WINDOW_MS, null,
                new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider provider, String dbName) {
                observedDbName.set(dbName);
                return true;
            }
        };

        // Seed the dir->logical mapping as happens when the DB opens.
        l.onDBCreated("00001", dbDir.toString());

        try {
            // Pass the path form, as RocksDB does in production.
            l.onTableFileDeleted(dbDir.toString(), "default", dbDir.resolve("000001.sst").toString());
            l.processDeferredSstDeletes();
            assertEquals("00001",
                         observedDbName.get(),
                         "delete guard/metadata sync must key off the resolved logical name");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileCreated_uploadFailure_doesNotThrow_andSubmitsToRetryQueue()
            throws Exception {
        // Exercises the *async provider upload-failure* path: the SST is staged successfully
        // (real file under a real root, so the hard-link pin succeeds) and the failure comes
        // from provider.uploadFile() inside the background upload worker — the exact path that
        // must be caught, not the hard-link staging failure.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-retry");
        Path dbDir = tmpRoot.resolve("hgstore-metadata");
        Files.createDirectories(dbDir);
        Path sst = dbDir.resolve("000008.sst");
        Files.write(sst, "sst-body".getBytes());

        // maxAttempts=0 → after the async upload fails, the queue gives up on the task straight
        // away, giving a deterministic, observable postcondition (no timing-dependent retry cycle).
        FailingUploadProvider provider = new FailingUploadProvider();
        try (CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(provider,
                0, 50L, 50L)) {
            CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                    List.of(tmpRoot.toString()), true, 0L, retryQueue);
            // Must NOT throw – the provider failure is handled asynchronously.
            l.onTableFileCreated("hgstore-metadata", "default", sst.toString(),
                                 Files.size(sst));

            // The pin succeeds, the async upload fails, and (maxAttempts=0) the queue gives up on
            // the task. Wait for the background worker to complete and assert the failure was
            // captured on the real provider-upload path.
            waitForCondition(() -> retryQueue.getGivenUpCount() > 0,
                             "async provider upload failure should be submitted to the "
                             + "retry queue and given up");

            assertEquals(1L,
                         retryQueue.getGivenUpCount(),
                         "Provider upload failure must be counted as given up");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileCreated_nonRetryableUploadFailure_givesUpWithoutRetry()
            throws Exception {
        // A NON-retryable provider error (e.g. bad credentials) must bypass the retry budget and be
        // given up on immediately, even though maxAttempts > 0. maxAttempts=3 proves it is not
        // retried.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-nonretry");
        Path dbDir = tmpRoot.resolve("hgstore-metadata");
        Files.createDirectories(dbDir);
        Path sst = dbDir.resolve("000002.sst");
        Files.write(sst, "sst-body".getBytes());

        NonRetryableUploadProvider provider = new NonRetryableUploadProvider();
        try (CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(provider,
                3, 50L, 5000L)) {
            CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                    List.of(tmpRoot.toString()), true, 0L, retryQueue);
            // Must NOT throw – the provider failure is handled asynchronously.
            l.onTableFileCreated("hgstore-metadata", "default", sst.toString(), Files.size(sst));

            waitForCondition(() -> retryQueue.getGivenUpCount() > 0,
                             "non-retryable upload failure should be given up immediately");

            assertEquals(1L,
                         retryQueue.getGivenUpCount(),
                         "non-retryable failure must be given up on directly (no retries)");
            // The first retry would fire after the 50ms initial delay; wait past it to prove no
            // whole-file retry was scheduled.
            Thread.sleep(300L);
            assertEquals(1, provider.uploadCalls.get(),
                         "non-retryable failure must not trigger any whole-file retry");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileCreated_uploadFailure_noRetryQueue_doesNotThrow() {
        // A listener without a retry queue: failure must still not throw (just logs).
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(
                new FailingUploadProvider(), List.of(DATA_ROOT));
        l.onTableFileCreated("hgstore-metadata", "default",
                                    DATA_ROOT + "/hgstore-metadata/000008.sst", 512L);
        // If we reached here without an exception the test passes.
    }

    @Test
    public void onTableFileCreated_isNonBlocking_returnsQuicklyWithSlowProvider() throws Exception {
        // Verify that onTableFileCreated does not block on provider.uploadFile(),
        // even when the provider sleeps for a long duration.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-nonblocking");
        Path metadataDir = tmpRoot.resolve("hgstore-metadata");
        Files.createDirectories(metadataDir);
        Path sst = metadataDir.resolve("000008.sst");
        Files.write(sst, "sst".getBytes());

        try {
            long sleepDurationMs = 1000L;  // Provider will sleep for 1 second
            SlowUploadProvider provider = new SlowUploadProvider(sleepDurationMs);
            CloudStorageEventListener l =
                    CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()));

            // Measure how long onTableFileCreated takes to return
            long startNs = System.nanoTime();
            l.onTableFileCreated("hgstore-metadata", "default", sst.toString(), 512L);
            long callDurationMs = (System.nanoTime() - startNs) / 1_000_000;

            // The callback must return in a fraction of the provider's sleep time.
            // Allow a small buffer (100ms) for overhead, but the bulk of the sleep
            // should happen asynchronously in the background executor.
            assertTrue(callDurationMs < 200L,
                       "onTableFileCreated should return quickly without blocking on upload; "
                       + "call took " + callDurationMs + "ms (provider sleeps for "
                       + sleepDurationMs + "ms)");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileDeleted_delegatesToProvider_withRelativeKey() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = new CloudStorageEventListener(provider, List.of(DATA_ROOT),
                true, CloudStorageEventListener.DEFAULT_READ_MISS_GUARD_WINDOW_MS, null,
                new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                return true;
            }
        };

        l.onTableFileDeleted("hgstore-metadata", "default",
                             DATA_ROOT + "/hgstore-metadata/000008.sst");
        l.processDeferredSstDeletes();

        assertEquals(1, provider.deletes.size());
        assertEquals("hgstore-metadata/000008.sst", provider.deletes.get(0));
    }

    // -----------------------------------------------------------------------
    // onDBCreated – uploads existing SST files from the DB directory
    // -----------------------------------------------------------------------

    @Test
    public void onDBCreated_uploadsExistingSstFiles() throws Exception {
        // Create a temporary directory that mimics a partition DB directory.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        Files.createFile(partitionDir.resolve("000001.sst"));
        Files.createFile(partitionDir.resolve("000002.sst"));

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()));

        try {
            // Trigger onDBCreated; startup backfill should upload both SST files synchronously.
            l.onDBCreated("0", partitionDir.toString());

            assertEquals(2, provider.uploads.size());
            List<String> remoteKeys = new ArrayList<>();
            for (String[] u : provider.uploads) {
                remoteKeys.add(u[1]);
            }
            // Keys should be relative (e.g. "0/000001.sst")
            for (String key : remoteKeys) {
                assertFalse(key.startsWith("/"), "Remote key must not start with '/': " + key);
                assertTrue(key.endsWith(".sst"), "Remote key must end with .sst: " + key);
            }
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBCreated_existingUploadFailure_doesNotThrow() throws Exception {
        // Upload failures during onDBCreated must NOT propagate: the session is already live in
        // dbSessionMap at this point, so throwing here would cause a split-brain where the
        // caller believes the open failed while other threads already use the DB successfully.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        Files.createFile(partitionDir.resolve("000001.sst"));

        FailingUploadProvider provider = new FailingUploadProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()));

        try {
            // Should not throw even though the upload fails.
            l.onDBCreated("0", partitionDir.toString());
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // metadata mirroring (upload order, CURRENT-last, prune, consistent restore)
    // -----------------------------------------------------------------------
    private static CloudStorageEventListener metadataListener(CloudStorageProvider provider) {
        return CloudStorageTestFactory.newListener(provider, List.of(DATA_ROOT), false, 0L,
                                                   null, new CloudSyncTracker());
    }

    private static MetadataSnapshot snapshot(List<String> options, List<String> ssts) {
        return new MetadataSnapshot("/hugegraph-store/storage/0", "/hugegraph-store/storage/0" + "_tmp",
                                    "CURRENT", "MANIFEST-000005", options, ssts);
    }

    @Test
    public void uploadMetadataSnapshot_uploadsReferencedSstsThenMetadataThenCurrentLast() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = metadataListener(provider);
        // The referenced SST is already durable in cloud → no re-upload, but metadata must follow.
        provider.putRemoteFile("0/000003.sst", "sst".getBytes());

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"));

        boolean durable = l.uploadMetadataSnapshot(provider, "0", snap);

        assertTrue(durable);
        List<String> keys = new ArrayList<>();
        for (String[] up : provider.uploads) {
            keys.add(up[1]);
        }
        // OPTIONS + MANIFEST are published before CURRENT; CURRENT is published last of all.
        assertEquals(List.of("0/OPTIONS-000004", "0/MANIFEST-000005", "0/CURRENT"), keys);
    }

    @Test
    public void uploadMetadataSnapshot_holdsManifestAndCurrentWhenReferencedSstUploadFails() {
        // Referenced SST is neither in cloud nor uploadable → the whole publish must be held.
        FailingUploadProvider provider = new FailingUploadProvider();
        CloudStorageEventListener l = metadataListener(provider);

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"));

        boolean durable = l.uploadMetadataSnapshot(provider, "0", snap);

        assertFalse(durable);
        // No MANIFEST/OPTIONS/CURRENT may be published while a referenced SST is not durable.
        for (String[] up : provider.uploads) {
            fail("nothing should have been published, but uploaded: " + up[1]);
        }
    }

    @Test
    public void uploadMetadataSnapshot_holdsPublishWhenSstConfirmationEpochTurnsStale() {
        CloudSyncTracker tracker = new CloudSyncTracker();
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                super.uploadFile(localPath, remoteKey);
                if ("0/000003.sst".equals(remoteKey)) {
                    // Force an epoch advance between upload and confirmation.
                    tracker.clearDb("0");
                }
            }
        };
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(DATA_ROOT), false, 0L, null, tracker);

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"));

        boolean durable = l.uploadMetadataSnapshot(provider, "0", snap);

        assertFalse(durable, "Stale epoch confirmation must hold metadata publish");
        List<String> keys = new ArrayList<>();
        for (String[] up : provider.uploads) {
            keys.add(up[1]);
        }
        assertEquals(List.of("0/000003.sst"),
                     keys,
                     "Only the SST upload may have happened before stale confirmation was dropped");
    }

    @Test
    public void uploadMetadataSnapshot_prunesSupersededRemoteMetadataButKeepsCurrentAndSst() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = metadataListener(provider);
        provider.putRemoteFile("0/000003.sst", "sst".getBytes());
        // Superseded remote metadata from a previous generation, plus files that must be kept.
        provider.putRemoteFile("0/MANIFEST-000001", "old".getBytes());
        provider.putRemoteFile("0/OPTIONS-000000", "old".getBytes());
        provider.putRemoteFile("0/CURRENT", "old".getBytes());

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"));

        assertTrue(l.uploadMetadataSnapshot(provider, "0", snap));

        assertTrue(provider.deletes.contains("0/MANIFEST-000001"));
        assertTrue(provider.deletes.contains("0/OPTIONS-000000"));
        // CURRENT (the pointer) and SST objects are never pruned by the metadata sync.
        assertFalse(provider.deletes.contains("0/CURRENT"));
        assertFalse(provider.deletes.contains("0/000003.sst"));
    }

    @Test
    public void preHydration_failsLoudlyWhenCurrentReferencesMissingManifest() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        // CURRENT points at a manifest that was never mirrored → restore must refuse to open.
        provider.putRemoteFile("0/CURRENT", "MANIFEST-000009\n".getBytes());

        try {
            l.onDBOpening("0", partitionDir.toString());
            fail("expected IllegalStateException for inconsistent restore");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Cloud restore inconsistent"));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void syncMetadataSnapshotInline_serializesPerDbAndRejectsOlderGeneration()
            throws Exception {
        MetadataSnapshot newer = generationSnapshot("hgstore-meta-newer", 200L);
        MetadataSnapshot older = generationSnapshot("hgstore-meta-older", 100L);

        CountDownLatch newerUploadEntered = new CountDownLatch(1);
        CountDownLatch releaseNewerUpload = new CountDownLatch(1);
        GenerationSequenceListener controlled = GenerationSequenceListener.builder()
                                                                         .withSnapshots(newer, older)
                                                                         .withBlock(
                                                                                 newerUploadEntered,
                                                                                   releaseNewerUpload)
                                                                         .build();
        @SuppressWarnings("resource")
        CapturingProvider provider = new CapturingProvider();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> newerResult = executor.submit(
                    () -> controlled.syncMetadataSnapshotInline(provider, "db0"));
            assertTrue(newerUploadEntered.await(2, TimeUnit.SECONDS),
                       "newer publish did not start");

            Future<Boolean> olderResult = executor.submit(
                    () -> controlled.syncMetadataSnapshotInline(provider, "db0"));

            // Second call must be blocked on the same db lock until the first publication exits.
            Thread.sleep(100L);
            assertEquals(1,
                         controlled.captureCalls(),
                         "second sync must not capture before first publish is released");

            releaseNewerUpload.countDown();

            assertTrue(newerResult.get(2, TimeUnit.SECONDS), "newer generation must publish");
            assertFalse(olderResult.get(2, TimeUnit.SECONDS), "older generation must be rejected");
            assertEquals(List.of(200L), controlled.publishedGenerations());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void syncMetadataSnapshotInline_allowsEqualGenerationRepublish() throws Exception {
        MetadataSnapshot first = generationSnapshot("hgstore-meta-equal-first", 200L);
        MetadataSnapshot second = generationSnapshot("hgstore-meta-equal-second", 200L);
        GenerationSequenceListener listener = GenerationSequenceListener.builder()
                                                                       .withSnapshots(first, second)
                                                                       .build();
        CapturingProvider provider = new CapturingProvider();

        assertTrue(listener.syncMetadataSnapshotInline(provider, "db0"),
                   "first publish should succeed");
        assertTrue(listener.syncMetadataSnapshotInline(provider, "db0"),
                   "equal-generation re-publish should remain allowed");
        assertEquals(List.of(200L, 200L),
                     listener.publishedGenerations(),
                     "both equal-generation publications should execute");
    }

    @Test
    public void syncMetadataSnapshotInline_rejectsOlderGenerationSequentially() throws Exception {
        MetadataSnapshot newer = generationSnapshot("hgstore-meta-seq-newer", 300L);
        MetadataSnapshot older = generationSnapshot("hgstore-meta-seq-older", 200L);
        GenerationSequenceListener listener = GenerationSequenceListener.sequential(newer, older);
        CapturingProvider provider = new CapturingProvider();

        assertTrue(listener.syncMetadataSnapshotInline(provider, "db0"),
                   "newer generation publish should succeed");
        assertFalse(listener.syncMetadataSnapshotInline(provider, "db0"),
                    "older generation must be rejected after newer publish");
        assertEquals(List.of(300L),
                     listener.publishedGenerations(),
                     "only newer generation should be published");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static MetadataSnapshot generationSnapshot(String tempDirPrefix, long generation)
            throws IOException {
        Path temp = Files.createTempDirectory(tempDirPrefix);
        String manifest = String.format("MANIFEST-%06d", generation);
        return new MetadataSnapshot(DATA_ROOT + "/db0", temp.toString(), "CURRENT", manifest,
                                    List.of(), List.of(), generation);
    }

    private static class GenerationSequenceListener extends CloudStorageEventListener {

        private final List<MetadataSnapshot> snapshots;
        private final long blockedGeneration;
        private final CountDownLatch blockedEntered;
        private final CountDownLatch blockedRelease;
        private final AtomicInteger captureCalls = new AtomicInteger();
        private final List<Long> publishedGenerations =
                Collections.synchronizedList(new ArrayList<>());

        private GenerationSequenceListener(List<MetadataSnapshot> snapshots,
                                           long blockedGeneration,
                                           CountDownLatch blockedEntered,
                                           CountDownLatch blockedRelease) {
            super(new CapturingProvider(), List.of(DATA_ROOT), true,
                  DEFAULT_READ_MISS_GUARD_WINDOW_MS,
                  null,
                  new CloudSyncTracker(), null, DEFAULT_METADATA_SYNC_DEBOUNCE_MS);
            this.snapshots = snapshots;
            this.blockedGeneration = blockedGeneration;
            this.blockedEntered = blockedEntered;
            this.blockedRelease = blockedRelease;
        }

        static Builder builder() {
            return new Builder();
        }

        static GenerationSequenceListener sequential(MetadataSnapshot... snapshots) {
            return builder().withSnapshots(snapshots).build();
        }

        int captureCalls() {
            return captureCalls.get();
        }

        List<Long> publishedGenerations() {
            return publishedGenerations;
        }

        @Override
        MetadataSnapshot captureMetadataSnapshot(String dbName) {
            int index = captureCalls.getAndIncrement();
            if (index >= snapshots.size()) {
                return null;
            }
            return snapshots.get(index);
        }

        @Override
        boolean uploadMetadataSnapshot(CloudStorageProvider provider, String dbName,
                                       MetadataSnapshot snapshot) {
            publishedGenerations.add(snapshot.getGeneration());
            if (snapshot.getGeneration() == blockedGeneration && blockedEntered != null
                && blockedRelease != null) {
                blockedEntered.countDown();
                try {
                    assertTrue(blockedRelease.await(2, TimeUnit.SECONDS),
                               "timed out waiting to release blocked publish");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("interrupted while waiting to release blocked publish");
                }
            }
            return true;
        }

        static class Builder {

            private List<MetadataSnapshot> snapshots = List.of();
            private long blockedGeneration = -1L;
            private CountDownLatch blockedEntered;
            private CountDownLatch blockedRelease;

            Builder withSnapshots(MetadataSnapshot... snapshots) {
                this.snapshots = List.of(snapshots);
                return this;
            }

            Builder withBlock(CountDownLatch entered,
                              CountDownLatch release) {
                this.blockedGeneration = 200L;
                this.blockedEntered = entered;
                this.blockedRelease = release;
                return this;
            }

            GenerationSequenceListener build() {
                return new GenerationSequenceListener(snapshots, blockedGeneration,
                                                      blockedEntered, blockedRelease);
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            for (File child : Objects.requireNonNull(f.listFiles())) {
                deleteRecursively(child);
            }
        }
        f.delete();
    }

    private void waitForCondition(BooleanSupplier condition, String timeoutMessage)
            throws InterruptedException {
        waitForCondition(condition, 2000L, timeoutMessage);
    }

    private void waitForCondition(BooleanSupplier condition, long timeoutMs,
                                  String timeoutMessage) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(10_000_000L);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted while waiting for async cloud callback");
            }
        }
        fail(timeoutMessage);
    }

    /**
     * Minimal {@link CloudStorageProvider} that records upload and delete calls.
     */
    static class CapturingProvider implements CloudStorageProvider {

        final List<String[]> uploads = Collections.synchronizedList(new ArrayList<>());
        final List<String> deletes = Collections.synchronizedList(new ArrayList<>());
        final Map<String, byte[]> remoteFiles = new HashMap<>();
        int listFilesCalls = 0;

        @Override
        public String providerName() {
            return "capturing";
        }

        @Override
        public void init(CloudStorageConfig config) {
        }

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            uploads.add(new String[]{localPath, remoteKey});
        }

        @SuppressWarnings("RedundantThrows")
        @Override
        public void deleteFile(String remoteKey) throws IOException {
            deletes.add(remoteKey);
        }

        @SuppressWarnings("RedundantThrows")
        @Override
        public boolean fileExists(String remoteKey) throws IOException {
            return remoteFiles.containsKey(remoteKey);
        }

        @SuppressWarnings("RedundantThrows")
        @Override
        public List<String> listFiles(String remoteDirPrefix) throws IOException {
            listFilesCalls++;
            List<String> result = new ArrayList<>();
            for (String key : remoteFiles.keySet()) {
                if (key.startsWith(remoteDirPrefix)) {
                    result.add(key);
                }
            }
            return result;
        }

        @Override
        public void downloadFile(String remoteKey, String localPath) throws IOException {
            byte[] data = remoteFiles.get(remoteKey);
            if (data == null) {
                throw new IOException("remote key not found: " + remoteKey);
            }
            Path p = Paths.get(localPath);
            Files.createDirectories(p.getParent());
            Files.write(p, data);
        }

        @SuppressWarnings("RedundantThrows")
        @Override
        public void close() throws IOException {
        }

        @SuppressWarnings("RedundantThrows")
        @Override
        public int deletePrefix(String remoteDirPrefix) throws IOException {
            List<String> result = new ArrayList<>();
            for (String key : remoteFiles.keySet()) {
                if (key.startsWith(remoteDirPrefix)) {
                    result.add(key);
                }
            }
            for (String key : result) {
                deletes.add(key);
                remoteFiles.remove(key);
            }
            return result.size();
        }

        void putRemoteFile(String key, byte[] content) {
            remoteFiles.put(key, content);
        }
    }

    static class FailingUploadProvider extends CapturingProvider {

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            throw new IOException("simulated upload failure");
        }
    }

    /** Upload always fails with a NON-retryable error (e.g. bad credentials / 403). */
    static class NonRetryableUploadProvider extends CapturingProvider {

        final AtomicInteger uploadCalls = new AtomicInteger();

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            uploadCalls.incrementAndGet();
            throw new CloudStorageNonRetryableException(
                    "Authentication failed; credentials invalid", null);
        }
    }

    /**
     * A {@link CloudStorageProvider} that sleeps during upload to simulate a slow provider.
     * Used to verify that {@code onTableFileCreated} is non-blocking and does not wait for
     * the provider's upload to complete.
     */
    static class SlowUploadProvider extends CapturingProvider {

        private final long sleepMs;

        SlowUploadProvider(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            // Record the upload call (parent class behavior)
            super.uploadFile(localPath, remoteKey);
            // Simulate a slow operation that would block if called synchronously
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("upload sleep interrupted", e);
            }
        }
    }

    /**
     * A provider whose tombstone existence check ({@link #fileExists}) always fails with an
     * {@link IOException}, simulating a cloud that cannot confirm whether the previous generation
     * was tombstoned. Exercises the {@code preHydrateDbFiles} catch-block that must fall back to
     * inspecting local CURRENT→MANIFEST self-consistency.
     */
    static class TombstoneCheckFailingProvider extends CapturingProvider {

        final AtomicInteger fileExistsCalls = new AtomicInteger();

        @Override
        public boolean fileExists(String remoteKey) throws IOException {
            fileExistsCalls.incrementAndGet();
            throw new IOException("simulated tombstone-check failure for " + remoteKey);
        }
    }

    @Test
    public void onDBOpening_downloadsMissingRemoteFiles() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        provider.putRemoteFile("0/CURRENT", "MANIFEST-000001".getBytes());
        provider.putRemoteFile("0/MANIFEST-000001", "manifest-body".getBytes());
        provider.putRemoteFile("0/000001.sst", "sst-body".getBytes());

        try {
            l.onDBOpening("0", partitionDir.toString());
            assertTrue(Files.exists(partitionDir.resolve("CURRENT")));
            assertTrue(Files.exists(partitionDir.resolve("MANIFEST-000001")));
            assertTrue(Files.exists(partitionDir.resolve("000001.sst")));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_downloadsMissingRemoteFiles_withStoreScopePrefix() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, new CloudSyncTracker(),
                "store-127.0.0.1_8501");
        provider.putRemoteFile("store-127.0.0.1_8501/0/CURRENT", "MANIFEST-000001".getBytes());
        provider.putRemoteFile("store-127.0.0.1_8501/0/MANIFEST-000001", "manifest-body".getBytes());
        provider.putRemoteFile("store-127.0.0.1_8501/0/000001.sst", "sst-body".getBytes());

        try {
            l.onDBOpening("0", partitionDir.toString());
            assertTrue(Files.exists(partitionDir.resolve("CURRENT")));
            assertTrue(Files.exists(partitionDir.resolve("MANIFEST-000001")));
            assertTrue(Files.exists(partitionDir.resolve("000001.sst")));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_adoptsNewerRemoteCurrent_noSilentRollback() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        // Stale local metadata at generation 5 (e.g. a partial disk rollback).
        Files.write(partitionDir.resolve("CURRENT"), "MANIFEST-000005".getBytes());
        Files.write(partitionDir.resolve("MANIFEST-000005"), "manifest-5".getBytes());

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        // Cloud holds a strictly newer generation 10.
        provider.putRemoteFile("0/CURRENT", "MANIFEST-000010".getBytes());
        provider.putRemoteFile("0/MANIFEST-000010", "manifest-10".getBytes());
        provider.putRemoteFile("0/000010.sst", "sst-10".getBytes());

        try {
            l.onDBOpening("0", partitionDir.toString());
            // The newer generation's manifest is hydrated ...
            assertTrue(Files.exists(partitionDir.resolve("MANIFEST-000010")),
                       "Newer remote MANIFEST must be hydrated");
            // ... and the fixed-name CURRENT pointer must be advanced to it (no silent rollback to
            // the older local generation that skip-on-exists would otherwise have preserved).
            assertEquals("MANIFEST-000010",
                         Files.readString(partitionDir.resolve("CURRENT")).trim(),
                         "CURRENT must point at the newer remote generation");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_keepsNewerLocalCurrent_doesNotRollBackToOlderCloud() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        // Local metadata is NEWER (generation 10) — recent writes not yet mirrored to cloud.
        Files.write(partitionDir.resolve("CURRENT"), "MANIFEST-000010".getBytes());
        Files.write(partitionDir.resolve("MANIFEST-000010"), "manifest-10".getBytes());

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        // Cloud lags at an older generation 5.
        provider.putRemoteFile("0/CURRENT", "MANIFEST-000005".getBytes());
        provider.putRemoteFile("0/MANIFEST-000005", "manifest-5".getBytes());

        try {
            l.onDBOpening("0", partitionDir.toString());
            assertEquals("MANIFEST-000010",
                         Files.readString(partitionDir.resolve("CURRENT")).trim(),
                         "Newer local CURRENT must be preserved; hydration must not roll the DB "
                         + "back to an older cloud generation");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_cloudUnreachable_failsWhenLocalMetadataInconsistent() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        // Orphan SST but NO valid CURRENT->MANIFEST lineage (partial/rolled-back directory).
        Files.write(partitionDir.resolve("000001.sst"), "orphan".getBytes());

        CapturingProvider provider = new CapturingProvider() {
            @Override
            public boolean fileExists(String remoteKey) {
                return false; // no tombstone
            }

            @Override
            public List<String> listFiles(String remoteDirPrefix) throws IOException {
                throw new IOException("cloud unreachable");
            }
        };
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);

        try {
            l.onDBOpening("0", partitionDir.toString());
            fail("Must fail loudly when cloud is unreachable and local metadata is inconsistent");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("self-consistent")
                       || expected.getMessage().contains("CURRENT"),
                       "Failure must cite the inconsistent local metadata: "
                       + expected.getMessage());
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_cloudUnreachable_proceedsWhenLocalMetadataConsistent() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        // Self-consistent local metadata: CURRENT -> a locally-present MANIFEST, plus an SST.
        Files.write(partitionDir.resolve("CURRENT"), "MANIFEST-000001".getBytes());
        Files.write(partitionDir.resolve("MANIFEST-000001"), "manifest".getBytes());
        Files.write(partitionDir.resolve("000001.sst"), "data".getBytes());

        CapturingProvider provider = new CapturingProvider() {
            @Override
            public boolean fileExists(String remoteKey) {
                return false;
            }

            @Override
            public List<String> listFiles(String remoteDirPrefix) throws IOException {
                throw new IOException("cloud unreachable");
            }
        };
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);

        try {
            // Must NOT throw: local state is self-consistent, so falling back is safe.
            l.onDBOpening("0", partitionDir.toString());
            assertTrue(Files.exists(partitionDir.resolve("CURRENT")),
                       "Local CURRENT must be preserved on the safe fallback path");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // Read-miss restores missing LIVE files to their original path (no ingest)
    // -----------------------------------------------------------------------

    @Test
    public void restoreMissingLiveFiles_restoresMissingLiveFileToOriginalPath() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        provider.putRemoteFile("0/000001.sst", "sst-body".getBytes());

        String localPath = partitionDir.resolve("000001.sst").toString();
        List<LiveSstFile> live = List.of(new LiveSstFile(localPath, "default"));

        try {
            int restored = l.restoreMissingLiveFiles(provider, "0", live);
            assertEquals(1, restored);
            // Restored to the EXACT original path so RocksDB finds it; no ingest is performed.
            assertTrue(Files.exists(partitionDir.resolve("000001.sst")));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void restoreMissingLiveFiles_routesEachFileToItsOwnPath_crossCf() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        provider.putRemoteFile("0/000001.sst", "g-body".getBytes());
        provider.putRemoteFile("0/000002.sst", "q-body".getBytes());

        // Two live files belonging to different column families.
        List<LiveSstFile> live = List.of(
                new LiveSstFile(partitionDir.resolve("000001.sst").toString(), "g"),
                new LiveSstFile(partitionDir.resolve("000002.sst").toString(), "q"));

        try {
            int restored = l.restoreMissingLiveFiles(provider, "0", live);
            assertEquals(2, restored);
            // Each file lands at its own path; content is not mixed across CFs.
            assertEquals("g-body",
                         new String(Files.readAllBytes(partitionDir.resolve("000001.sst"))));
            assertEquals("q-body",
                         new String(Files.readAllBytes(partitionDir.resolve("000002.sst"))));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void restoreMissingLiveFiles_skipsFilesPresentLocally() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        Files.write(partitionDir.resolve("000001.sst"), "already-here".getBytes());

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        provider.putRemoteFile("0/000001.sst", "cloud-body".getBytes());

        List<LiveSstFile> live = List.of(
                new LiveSstFile(partitionDir.resolve("000001.sst").toString(), "default"));

        try {
            int restored = l.restoreMissingLiveFiles(provider, "0", live);
            assertEquals(0, restored);
            // Present local file is untouched (not overwritten from cloud).
            assertEquals("already-here",
                         new String(Files.readAllBytes(partitionDir.resolve("000001.sst"))));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void readMissGuard_skipsRepeatedAttemptsWithinWindow() {
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(new CapturingProvider(), List.of(DATA_ROOT),
                                                    true, 60_000L);
        assertTrue(l.shouldAttemptReadMissHydration("0", "default"));
        assertFalse(l.shouldAttemptReadMissHydration("0", "default"));
        // A different table is not throttled by the first table's attempt.
        assertTrue(l.shouldAttemptReadMissHydration("0", "other"));
    }

    @Test
    public void readMissGuard_admitsExactlyOnePerWindowUnderConcurrency() throws Exception {
        // The guard must admit ONLY ONE caller per DB/table window even under a concurrent read
        // storm; a non-atomic get-then-put would let multiple callers pass on the race boundary.
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(new CapturingProvider(), List.of(DATA_ROOT),
                                                    true, 60_000L);

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger admitted = new AtomicInteger(0);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        ready.countDown();
                        go.await();
                        if (l.shouldAttemptReadMissHydration("0", "default")) {
                            admitted.incrementAndGet();
                        }
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS), "Workers must be ready");
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "Workers must finish");
        } finally {
            pool.shutdownNow();
        }
        assertEquals(1,
                     admitted.get(),
                     "Exactly one caller may be admitted within a single guard window");
    }

    @Test
    public void deleteGuard_holdsWhenLiveFileNotConfirmedAndUploadFails() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-guard");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        // A live file that exists locally but whose upload will fail.
        Path liveLocal = partitionDir.resolve("000009.sst");
        Files.write(liveLocal, "live".getBytes());

        CloudSyncTracker tracker = new CloudSyncTracker();
        FailingUploadProvider provider = new FailingUploadProvider();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, tracker);

        List<LiveSstFile> live = List.of(new LiveSstFile(liveLocal.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertFalse(durable, "Guard must report the live set as NOT durable");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void deleteGuard_passesWhenAllLiveFilesConfirmed() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-guard");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        Path liveLocal = partitionDir.resolve("000009.sst");
        Files.write(liveLocal, "live".getBytes());

        CloudSyncTracker tracker = new CloudSyncTracker();
        // Pre-mark the live file as already confirmed in cloud.
        tracker.markConfirmed("0", liveLocal.toString());
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, tracker);

        List<LiveSstFile> live = List.of(new LiveSstFile(liveLocal.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertTrue(durable, "Guard must pass when the whole live set is confirmed");
            // No upload needed since it was already confirmed.
            assertEquals(0, provider.uploads.size());
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void deleteGuard_uploadsUnconfirmedLiveFileThenPasses() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-guard");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        Path liveLocal = partitionDir.resolve("000009.sst");
        Files.write(liveLocal, "live".getBytes());

        CloudSyncTracker tracker = new CloudSyncTracker();
        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, tracker);

        List<LiveSstFile> live = List.of(new LiveSstFile(liveLocal.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertTrue(durable);
            // The unconfirmed-but-present live file was uploaded and then marked confirmed.
            assertEquals(1, provider.uploads.size());
            assertEquals("0/000009.sst", provider.uploads.get(0)[1]);
            assertTrue(tracker.isConfirmed("0", liveLocal.toString()));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void deleteGuard_blocksOldSstDelete_whenInputsConfirmedButMergedOutputMissing()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-guard");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        // Compaction inputs SST1/SST2 were uploaded previously and are still present locally.
        Path sst1 = partitionDir.resolve("000001.sst");
        Path sst2 = partitionDir.resolve("000002.sst");
        Files.write(sst1, "sst1".getBytes());
        Files.write(sst2, "sst2".getBytes());

        // Compaction output exists in RocksDB live-set metadata but is missing both locally and
        // in cloud (upload failed/lost). This is the dangerous window we must guard.
        Path merged = partitionDir.resolve("000010.sst");

        CloudSyncTracker tracker = new CloudSyncTracker();
        tracker.markConfirmed("0", sst1.toString());
        tracker.markConfirmed("0", sst2.toString());

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, tracker);
        provider.putRemoteFile("0/000001.sst", "sst1".getBytes());
        provider.putRemoteFile("0/000002.sst", "sst2".getBytes());

        // Post-compaction live set contains only MERGED_SST.
        List<LiveSstFile> live = List.of(new LiveSstFile(merged.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertFalse(durable, "Merged output is not durable; old SST delete must be blocked");
            // MERGED_SST is absent locally, so guard cannot self-heal by upload.
            assertEquals(0, provider.uploads.size());
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // MANIFEST-before-delete invariant
    // -----------------------------------------------------------------------

    /**
     * Captures call-order between {@code syncMetadataSnapshotInline} and {@code provider.deleteFile}.
     * Overrides {@code syncMetadataSnapshotInline} so no live RocksDB session is required.
     */
    static class OrderingListener extends CloudStorageEventListener {

        final List<String> callOrder = Collections.synchronizedList(new ArrayList<>());
        private final boolean syncResult;

        OrderingListener(CloudStorageProvider provider, String dataRoot, CloudSyncTracker tracker,
                         boolean syncResult) {
            super(provider, List.of(dataRoot), false, 0L, null, tracker, null,
                  DEFAULT_METADATA_SYNC_DEBOUNCE_MS);
            this.syncResult = syncResult;
        }

        @Override
        boolean syncMetadataSnapshotInline(CloudStorageProvider provider, String dbName) {
            callOrder.add("sync");
            return syncResult;
        }
    }

    /**
     * Verifies the MANIFEST-before-delete invariant:
     * {@code syncMetadataSnapshotInline} must be called and succeed before the superseded SST is
     * deleted from cloud.
     *
     * <p>Scenario: compaction merged SST1+SST2 into MERGED_SST. MERGED_SST was already uploaded
     * and confirmed. When {@code onTableFileDeleted} fires for SST1 an updated MANIFEST+CURRENT
     * must be published first, then SST1 deleted. A crash between those two steps leaves the
     * cluster in a recoverable state.
     */
    @Test
    public void onTableFileDeleted_publishesUpdatedManifestBeforeDeletingSupersededSst() {
        String sst1Remote = "db0/000001.sst";
        // No real RocksDB session for "db0" → getLiveSstFiles() returns empty
        // → ensureLiveSetUploaded passes trivially.
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void deleteFile(String remoteKey) throws IOException {
                // Record delete after any sync call already recorded by the listener.
                super.deleteFile(remoteKey);
            }
        };
        provider.putRemoteFile(sst1Remote, "sst1".getBytes());

        OrderingListener l = getOrderingListener(provider);

        assertEquals(List.of("sync"), l.callOrder, "sync must have been called once");
        assertTrue(provider.deletes.contains(sst1Remote), "SST1 must be deleted from cloud");
    }

    private static @NotNull OrderingListener getOrderingListener(CapturingProvider provider) {
        CloudSyncTracker tracker = new CloudSyncTracker();
        OrderingListener l =
                new OrderingListener(provider, DATA_ROOT, tracker, /* syncResult */ true) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                boolean result = super.syncMetadataSnapshotInline(p, dbName);
                // Verify no delete has happened yet when sync fires.
                assertTrue(provider.deletes.isEmpty(), "delete must not precede sync");
                return result;
            }
        };

        l.onTableFileDeleted("db0", "default", DATA_ROOT + "/db0/000001.sst");
        l.processDeferredSstDeletes();
        return l;
    }

    /**
     * When {@code syncMetadataSnapshotInline} fails (MANIFEST not updated), the SST delete must be
     * skipped entirely to avoid leaving an irrecoverable state in cloud.
     */
    @Test
    public void onTableFileDeleted_skipsDeleteWhenMetadataSyncFails() {
        String sst1Remote = "db0/000001.sst";
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile(sst1Remote, "sst1".getBytes());

        CloudSyncTracker tracker = new CloudSyncTracker();
        // syncResult=false: metadata sync fails → delete must be suppressed.
        OrderingListener l =
                new OrderingListener(provider, DATA_ROOT, tracker, /* syncResult */ false);

        l.onTableFileDeleted("db0", "default", DATA_ROOT + "/db0/000001.sst");
        l.processDeferredSstDeletes();

        // The held delete is retried in the background, so sync may have been attempted again.
        assertFalse(l.callOrder.isEmpty(), "sync must have been called");
        assertTrue(provider.deletes.isEmpty(), "SST must NOT be deleted when metadata sync fails");
    }

    @Test
    public void onTableFileDeleted_deferredDeleteIsRetriedAfterMetadataRecovers() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-deferred-delete");
        Path dbDir = tmpRoot.resolve("db0");
        Files.createDirectories(dbDir);
        Path oldSst = dbDir.resolve("000001.sst");

        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("db0/000001.sst", "sst1".getBytes());

        AtomicBoolean metadataHealthy = new AtomicBoolean(false);
        CloudStorageEventListener l = new CloudStorageEventListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            List<LiveSstFile> currentLiveSstFiles(String dbName) {
                return List.of();
            }

            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                return metadataHealthy.get();
            }
        };

        try {
            l.onTableFileDeleted("db0", "default", oldSst.toString());
            assertTrue(provider.deletes.isEmpty(),
                       "Initial delete must be deferred while metadata sync is failing");

            metadataHealthy.set(true);
            waitForCondition(() -> provider.deletes.contains("db0/000001.sst"),
                             "Deferred delete should be retried once metadata sync recovers");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileDeleted_holdsDeleteWhenLiveSetConfirmationEpochTurnsStale()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-delete-stale-epoch");
        Path dbDir = tmpRoot.resolve("db0");
        Files.createDirectories(dbDir);
        Path oldSst = dbDir.resolve("000001.sst");
        Path liveSst = dbDir.resolve("000010.sst");
        Files.write(liveSst, "live".getBytes());

        CloudSyncTracker tracker = new CloudSyncTracker();
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                super.uploadFile(localPath, remoteKey);
                if ("db0/000010.sst".equals(remoteKey)) {
                    // Simulate DB recreation between upload completion and confirmation.
                    tracker.clearDb("db0");
                }
            }
        };
        provider.putRemoteFile("db0/000001.sst", "old".getBytes());

        CloudStorageEventListener l = new CloudStorageEventListener(provider,
                List.of(tmpRoot.toString()), true, 0L, null, tracker, null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            List<LiveSstFile> currentLiveSstFiles(String dbName) {
                return List.of(new LiveSstFile(liveSst.toString(), "default"));
            }

            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                return true;
            }
        };

        try {
            l.onTableFileDeleted("db0", "default", oldSst.toString());
            l.processDeferredSstDeletes();

            assertTrue(provider.deletes.isEmpty(),
                       "Delete must be held when live-set confirmation is stale");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /**
     * Drives the full {@link CloudStorageEventListener#onTableFileDeleted} path with a live set that
     * is NOT durable (a compaction-output SST that is neither confirmed nor present in cloud). The
     * delete guard must block the superseded-input delete BEFORE any MANIFEST publish. Unlike
     * {@link #onTableFileDeleted_publishesUpdatedManifestBeforeDeletingSupersededSst} (which relies
     * on an empty live set so the guard passes trivially), this exercises the guard for real, so a
     * regression that weakens/removes the live-set durability precondition is caught.
     */
    @Test
    public void onTableFileDeleted_blocksDeleteWhenLiveSetNotDurable() {
        String sst1Remote = "db0/000001.sst";
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile(sst1Remote, "sst1".getBytes());

        CloudSyncTracker tracker = new CloudSyncTracker();
        List<String> syncCalls = Collections.synchronizedList(new ArrayList<>());
        // Compaction output 000010.sst is live but neither confirmed nor present locally / in cloud,
        // so the live set is not durable.
        CloudStorageEventListener l = new CloudStorageEventListener(provider,
                List.of(DATA_ROOT), false, 0L, null, tracker, null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            List<LiveSstFile> currentLiveSstFiles(String dbName) {
                return List.of(new LiveSstFile(DATA_ROOT + "/db0/000010.sst", "default"));
            }

            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                syncCalls.add("sync");
                return true;
            }
        };

        l.onTableFileDeleted("db0", "default", DATA_ROOT + "/db0/000001.sst");
        l.processDeferredSstDeletes();

        assertTrue(provider.deletes.isEmpty(),
                   "Superseded SST must NOT be deleted while the live set is not durable in cloud");
        assertTrue(syncCalls.isEmpty(),
                   "MANIFEST publish must not be attempted when the durability guard blocks first");
    }

    // -----------------------------------------------------------------------
    // onDBDeleteBegin / onDBDeleted — stale-data guard
    // -----------------------------------------------------------------------

    @Test
    public void onDBDeleteBegin_writesTombstoneToCloud() throws Exception {
        // Use a writable temp root: onDBDeleteBegin now durably persists (and fsyncs) a local
        // pending-delete marker before the tombstone, and HOLDS the delete if that fails.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-deletebegin");
        try {
            CapturingProvider provider = new CapturingProvider();

            CloudStorageEventListener l =
                    CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()));
            l.onDBDeleteBegin("mydb", tmpRoot.resolve("mydb").toString());

            // Tombstone key is now a sibling of the data prefix, not inside it.
            String expectedTombstone = "mydb" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
            boolean tombstoneUploaded = provider.uploads.stream()
                    .anyMatch(pair -> expectedTombstone.equals(pair[1]));
            assertTrue(tombstoneUploaded,
                       "Tombstone must be uploaded as a sibling of the cloud prefix");
            assertTrue(l.hasPendingDeleteMarker("mydb"),
                       "A durable pending-delete marker must be written");
            assertTrue(l.isDeleteMarkerHealthy(), "Delete-marker health must remain healthy");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBDeleteBegin_tombstoneKeyRespectsStoreScopePrefix() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-deletebegin-scoped");
        try {
            CapturingProvider provider = new CapturingProvider();

            CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                    List.of(tmpRoot.toString()), true, 0L, null, new CloudSyncTracker(),
                    "store-127.0.0.1_8501");
            l.onDBDeleteBegin("mydb", tmpRoot.resolve("mydb").toString());

            String expectedTombstone = "store-127.0.0.1_8501/mydb"
                                       + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
            boolean tombstoneUploaded = provider.uploads.stream()
                    .anyMatch(pair -> expectedTombstone.equals(pair[1]));
            assertTrue(tombstoneUploaded, "Tombstone must include store scope prefix");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBDeleteBegin_holdsDeleteAndDegradesHealth_whenMarkerNotDurable() throws Exception {
        // If the local pending-delete marker cannot be durably persisted (e.g. the data root is not
        // writable), the delete MUST be held (throw) rather than proceeding unguarded, and the
        // delete-marker health signal must flip to degraded.
        CapturingProvider provider = new CapturingProvider();

        // A data root pointing at a plain FILE makes createDirectories(<root>/.cloud-pending-delete)
        // fail deterministically on every platform.
        Path notADir = Files.createTempFile("hgstore-notadir", ".tmp");
        try {
            CloudStorageEventListener l =
                    CloudStorageTestFactory.newListener(provider, List.of(notADir.toString()));
            try {
                l.onDBDeleteBegin("mydb", notADir.resolve("mydb").toString());
                fail("Expected delete to be held when the marker cannot be durably persisted");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("marker"),
                           "Message should explain the held delete: " + expected.getMessage());
            }
            assertFalse(l.isDeleteMarkerHealthy(),
                        "Delete-marker health must be degraded after a persistence failure");
            // The unguarded tombstone/purge must NOT have run.
            assertTrue(provider.uploads.isEmpty(),
                       "No tombstone must be uploaded when the delete is held");
        } finally {
            Files.deleteIfExists(notADir);
        }
    }

    @Test
    public void onDBDeleted_purgesAllRemoteObjectsUnderPrefix() {
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("mydb/000001.sst", "sst".getBytes());
        provider.putRemoteFile("mydb/CURRENT", "MANIFEST-1".getBytes());
        provider.putRemoteFile("mydb/MANIFEST-000001", "body".getBytes());
        // Tombstone is now a sibling of the data prefix, not inside it.
        provider.putRemoteFile("mydb" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX,
                               "deleted".getBytes());

        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(DATA_ROOT));
        l.onDBDeleted("mydb", DATA_ROOT + "/mydb");

        // Data prefix objects are purged by deletePrefix; tombstone is deleted individually.
        List<String> expectedDeleted = List.of(
                "mydb/000001.sst", "mydb/CURRENT", "mydb/MANIFEST-000001",
                "mydb" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX);
        for (String key : expectedDeleted) {
            assertTrue(provider.deletes.contains(key),
                       "Remote object must be deleted after DB destroy: " + key);
        }
    }

    @Test
    public void onDBDeleted_preservesTombstoneWhenPurgeIncomplete() {
        // If the prefix purge is incomplete (e.g. S3 returns a truncated listing without a
        // continuation token, which deletePrefix now surfaces as an IOException), stale objects may
        // remain. The tombstone MUST be preserved so a later open still blocks re-hydration of that
        // stale data instead of resurrecting it as live.
        String tombstoneKey = "mydb" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public int deletePrefix(String remoteDirPrefix) throws IOException {
                throw new IOException("simulated incomplete purge (truncated listing, no token)");
            }
        };
        provider.putRemoteFile("mydb/000001.sst", "sst".getBytes());
        provider.putRemoteFile(tombstoneKey, "deleted".getBytes());

        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(DATA_ROOT));
        l.onDBDeleted("mydb", DATA_ROOT + "/mydb");

        assertFalse(provider.deletes.contains(tombstoneKey),
                    "Tombstone must be preserved when the prefix purge is incomplete");
    }

    @Test
    public void onDBDeleted_clearsSyncTrackerState() {
        CloudSyncTracker tracker = new CloudSyncTracker();
        tracker.markConfirmed("mydb", DATA_ROOT + "/mydb/000001.sst");
        assertEquals(1L, tracker.confirmedCount("mydb"));

        CapturingProvider provider = new CapturingProvider();

        CloudStorageEventListener l = CloudStorageTestFactory.newListener(provider,
                List.of(DATA_ROOT), true, 0L, null, tracker);
        l.onDBDeleted("mydb", DATA_ROOT + "/mydb");

        assertEquals(0L,
                     tracker.confirmedCount("mydb"),
                     "Sync tracker must be cleared for the deleted DB");
    }

    @Test
    public void onDBOpening_skipsHydrationWhenTombstonePresent() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("mydb");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        // Populate cloud with stale data from a previous deleted generation + sibling tombstone.
        provider.putRemoteFile("mydb/000001.sst", "stale-sst".getBytes());
        provider.putRemoteFile("mydb/CURRENT", "MANIFEST-000001".getBytes());
        // Tombstone is a sibling key outside the data prefix.
        provider.putRemoteFile("mydb" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX,
                               "deleted".getBytes());

        try {
            l.onDBOpening("mydb", partitionDir.toString());

            // Hydration must be skipped — stale SST and CURRENT must NOT be downloaded.
            assertFalse(Files.exists(partitionDir.resolve("000001.sst")),
                        "Stale SST must not be hydrated when tombstone is present");
            assertFalse(Files.exists(partitionDir.resolve("CURRENT")),
                        "Stale CURRENT must not be hydrated when tombstone is present");
            // All stale remote objects in the data prefix must be purged.
            assertTrue(provider.deletes.contains("mydb/000001.sst"),
                       "Stale SST must be purged");
            assertTrue(provider.deletes.contains("mydb/CURRENT"),
                       "Stale CURRENT must be purged");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_proceedsNormallyWhenNoTombstone() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("mydb");
        Files.createDirectories(partitionDir);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        // Normal cloud state — no tombstone.
        provider.putRemoteFile("mydb/000001.sst", "sst-body".getBytes());
        provider.putRemoteFile("mydb/CURRENT", "MANIFEST-000001".getBytes());
        provider.putRemoteFile("mydb/MANIFEST-000001", "manifest".getBytes());

        try {
            l.onDBOpening("mydb", partitionDir.toString());

            assertTrue(Files.exists(partitionDir.resolve("000001.sst")),
                       "SST must be hydrated in normal open");
            assertTrue(Files.exists(partitionDir.resolve("CURRENT")),
                       "CURRENT must be hydrated in normal open");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void deleteAndRecreate_newDbDoesNotIngestDeletedData() throws Exception {
        // Simulate the full delete-then-recreate lifecycle:
        // 1. DB is created and writes data to cloud.
        // 2. DB is deleted → tombstone written, cloud purged.
        // 3. DB is recreated at the same path → hydration must find empty prefix, not stale data.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path dbDir = tmpRoot.resolve("graph0");
        Files.createDirectories(dbDir);

        CapturingProvider provider = new CapturingProvider();
        // Stale objects from the old generation.
        provider.putRemoteFile("graph0/000001.sst", "old-data".getBytes());
        provider.putRemoteFile("graph0/CURRENT", "MANIFEST-000001".getBytes());

        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);

        // Step 1: begin delete — tombstone uploaded as a sibling key.
        l.onDBDeleteBegin("graph0", dbDir.toString());
        boolean tombstoneUploaded = provider.uploads.stream()
                .anyMatch(p -> p[1].equals("graph0" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX));
        assertTrue(tombstoneUploaded, "Tombstone must be uploaded during deleteBegin");

        // Simulate tombstone appearing in cloud (as it would after a real upload).
        provider.putRemoteFile("graph0" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX,
                               "deleted".getBytes());

        // Step 2: deletion complete — cloud data prefix purged, then tombstone deleted.
        l.onDBDeleted("graph0", dbDir.toString());
        assertTrue(provider.deletes.contains("graph0/000001.sst"), "Old SST must be deleted");
        assertTrue(provider.deletes.contains("graph0/CURRENT"), "Old CURRENT must be deleted");
        assertTrue(provider.deletes.contains(
                           "graph0" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX),
                   "Tombstone must be deleted after purge");

        // Step 3: simulate a crash-recovery scenario where the tombstone guard is needed.
        // The tombstone is re-injected alongside stale SST objects (mimicking a case where
        // onDBDeleted preserved the tombstone because the cloud purge partially failed).
        // onDBOpening must detect the tombstone, purge stale data, clean the tombstone, and
        // return without hydrating any files into the recreated DB directory.
        String tombstoneKey = "graph0" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        // Clear the call-tracking list so only step-3 actions contribute to the assertion —
        // step 2's onDBDeleted already appended tombstoneKey to provider.deletes, which would
        // make the tombstone-cleanup assertion pass trivially without step 3 doing any work.
        provider.deletes.clear();
        provider.putRemoteFile(tombstoneKey, "deleted".getBytes());
        provider.putRemoteFile("graph0/000001.sst", "stale-data".getBytes());
        provider.putRemoteFile("graph0/CURRENT", "MANIFEST-000001".getBytes());

        Files.createDirectories(dbDir);
        l.onDBOpening("graph0", dbDir.toString());

        // Tombstone guard must have fired: stale files must NOT be hydrated locally.
        assertFalse(Files.exists(dbDir.resolve("000001.sst")),
                    "Stale SST must not be hydrated into recreated DB dir");
        assertFalse(Files.exists(dbDir.resolve("CURRENT")),
                    "Stale CURRENT must not be hydrated into recreated DB dir");
        // Tombstone must have been cleaned up specifically by step 3's preHydrateDbFiles.
        // provider.deletes was cleared before this step, so this proves step 3 called deleteFile.
        assertTrue(provider.deletes.contains(tombstoneKey),
                   "Tombstone must be deleted by preHydrateDbFiles in step 3");

        deleteRecursively(tmpRoot.toFile());
    }

    /**
     * When the tombstone existence check itself throws (cloud unreachable) AND the local metadata
     * is not self-consistent (no valid CURRENT→MANIFEST lineage), the DB must NOT boot: generation
     * state is unknowable, so silently opening from orphan local state could resurrect deleted
     * data. {@code preHydrateDbFiles} must fail loudly with an {@link IllegalStateException}.
     */
    @Test
    public void onDBOpening_tombstoneCheckFailsAndLocalMetadataInconsistent_blocksOpen()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path dbDir = tmpRoot.resolve("graph0");
        Files.createDirectories(dbDir);
        // Orphan SST with NO CURRENT — hasInconsistentLocalMetadata(root) returns true.
        Files.write(dbDir.resolve("000001.sst"), "orphan".getBytes());

        TombstoneCheckFailingProvider provider = new TombstoneCheckFailingProvider();
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);

        try {
            l.onDBOpening("graph0", dbDir.toString());
            fail("Expected IllegalStateException when tombstone check fails and local metadata "
                 + "is not self-consistent");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not self-consistent"),
                       "Message must explain the DB open was blocked for safety, got: "
                       + e.getMessage());
        } finally {
            assertTrue(provider.fileExistsCalls.get() > 0,
                       "Tombstone check must have been attempted");
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /**
     * When the tombstone existence check throws (cloud unreachable) but the local metadata IS
     * self-consistent (valid CURRENT→MANIFEST lineage present), it is safe to open from local
     * state: {@code preHydrateDbFiles} must swallow the failure, log a warning, and proceed
     * without throwing.
     */
    @Test
    public void onDBOpening_tombstoneCheckFailsButLocalMetadataConsistent_proceeds()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path dbDir = tmpRoot.resolve("graph0");
        Files.createDirectories(dbDir);
        // Self-consistent local state: CURRENT points to an existing MANIFEST.
        Files.write(dbDir.resolve("MANIFEST-000001"), "manifest".getBytes());
        Files.write(dbDir.resolve("CURRENT"), "MANIFEST-000001".getBytes());

        TombstoneCheckFailingProvider provider = new TombstoneCheckFailingProvider();
        // No remote files: after the fall-through, listRemoteKeys is empty and hydration is a no-op.
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);

        try {
            // Must NOT throw despite the tombstone-check IOException.
            l.onDBOpening("graph0", dbDir.toString());
            assertTrue(provider.fileExistsCalls.get() > 0,
                       "Tombstone check must have been attempted");
            // Self-consistent local state must be left intact.
            assertTrue(Files.exists(dbDir.resolve("CURRENT")), "Local CURRENT must remain");
            assertTrue(Files.exists(dbDir.resolve("MANIFEST-000001")),
                       "Local MANIFEST must remain");
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // PR review fixes: pending-delete / truncate purge safety, open ordering, backoff
    // -----------------------------------------------------------------------

    /** Provider whose {@code deletePrefix} can be switched to fail (simulated cloud outage). */
    static class ToggleablePurgeProvider extends CapturingProvider {

        volatile boolean failPurge;

        @Override
        public int deletePrefix(String remoteDirPrefix) throws IOException {
            if (failPurge) {
                throw new IOException("simulated purge failure");
            }
            return super.deletePrefix(remoteDirPrefix);
        }

        @Override
        public synchronized void putRemoteFile(String key, byte[] content) {
            super.putRemoteFile(key, content);
        }
    }

    @Test
    public void managesRemoteState_isTrue() {
        assertTrue(listener.managesRemoteState());
    }

    @Test
    public void staleDeletePurgeRetry_standsDownOnceTheDbIsCreatedAgain() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-stale-delete-retry");
        Path dbDir = tmpRoot.resolve("g1");
        Files.createDirectories(dbDir);
        ToggleablePurgeProvider provider = new ToggleablePurgeProvider();
        provider.putRemoteFile("g1/000001.sst", "old".getBytes());
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), false);
        try {
            provider.failPurge = true;
            l.onDBDeleteBegin("g1", dbDir.toString());
            l.onDBDeleted("g1", dbDir.toString());
            assertTrue(l.hasPendingDeleteMarker("g1"), "failed purge must leave the marker");
            long epochAtDelete = l.prefixEpoch("g1");

            // The graph is re-created at the same prefix and uploads new data; the cloud is back.
            l.onDBCreated("g1", dbDir.toString());
            assertTrue(l.prefixEpoch("g1") > epochAtDelete,
                       "creating a DB must invalidate scheduled purges for the prefix");
            provider.failPurge = false;
            provider.putRemoteFile("g1/000002.sst", "new".getBytes());

            // The first retry fires after ~500ms: it must NOT purge the new DB's objects.
            LockSupport.parkNanos(1_500_000_000L);
            assertTrue(provider.remoteFiles.containsKey("g1/000002.sst"),
                       "A stale delete-purge retry must not delete a re-created DB's data");
            assertTrue(provider.remoteFiles.containsKey("g1/000001.sst"),
                       "The cancelled retry must not purge at all");
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_blocksWhileDeleteCleanupFails_evenWithHydrationDisabled()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-open-blocked");
        Path dbDir = tmpRoot.resolve("g2");
        Files.createDirectories(dbDir);
        ToggleablePurgeProvider provider = new ToggleablePurgeProvider();
        provider.putRemoteFile("g2/000001.sst", "old".getBytes());
        // Hydration disabled: the pending-delete guard must still run.
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), false);
        try {
            provider.failPurge = true;
            l.onDBDeleteBegin("g2", dbDir.toString());
            l.onDBDeleted("g2", dbDir.toString());

            try {
                l.onDBOpening("g2", dbDir.toString());
                fail("Open must be blocked while the remote delete cleanup is unconfirmed");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("g2"), expected.getMessage());
            }
            assertTrue(l.hasPendingDeleteMarker("g2"), "marker must persist while blocked");

            // Cloud recovers: the next open finishes the cleanup first, then proceeds.
            provider.failPurge = false;
            l.onDBOpening("g2", dbDir.toString());
            assertFalse(l.hasPendingDeleteMarker("g2"));
            assertFalse(provider.remoteFiles.containsKey("g2/000001.sst"));
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /** Keeps the tombstone in {@code remoteFiles} so its presence and removal can be asserted. */
    static class TombstoneTrackingProvider extends ToggleablePurgeProvider {

        volatile boolean failTombstoneDelete;

        @Override
        public synchronized void uploadFile(String localPath, String remoteKey) throws IOException {
            super.uploadFile(localPath, remoteKey);
            if (remoteKey.endsWith(CloudStorageEventListener.DB_TOMBSTONE_SUFFIX)) {
                remoteFiles.put(remoteKey, "deleted".getBytes());
            }
        }

        @Override
        public synchronized void deleteFile(String remoteKey) throws IOException {
            if (failTombstoneDelete
                && remoteKey.endsWith(CloudStorageEventListener.DB_TOMBSTONE_SUFFIX)) {
                throw new IOException("simulated tombstone delete failure");
            }
            super.deleteFile(remoteKey);
            remoteFiles.remove(remoteKey);
        }

        @Override
        public synchronized boolean fileExists(String remoteKey) throws IOException {
            return super.fileExists(remoteKey);
        }

        @Override
        public synchronized int deletePrefix(String remoteDirPrefix) throws IOException {
            return super.deletePrefix(remoteDirPrefix);
        }
    }

    /**
     * A tombstone that outlived its pending-delete marker would make a later open purge the live
     * objects of a DB re-created at the same prefix. So a failed tombstone delete must keep the
     * marker, and with it the open blocked, until the tombstone is gone.
     */
    @Test
    public void failedTombstoneDelete_keepsMarker_andBlocksOpenUntilItSucceeds() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-tombstone-delete-fails");
        Path dbDir = tmpRoot.resolve("g3");
        Files.createDirectories(dbDir);
        TombstoneTrackingProvider provider = new TombstoneTrackingProvider();
        provider.putRemoteFile("g3/000001.sst", "old".getBytes());
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), false);
        try {
            provider.failTombstoneDelete = true;
            l.onDBDeleteBegin("g3", dbDir.toString());
            l.onDBDeleted("g3", dbDir.toString());
            assertFalse(provider.remoteFiles.containsKey("g3/000001.sst"), "prefix is purged");
            assertTrue(provider.remoteFiles.containsKey("g3_DELETED"), "tombstone delete failed");
            assertTrue(l.hasPendingDeleteMarker("g3"),
                       "the marker must outlive a tombstone whose delete failed");

            try {
                l.onDBOpening("g3", dbDir.toString());
                fail("Open must be blocked while the tombstone cannot be deleted");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("g3"), expected.getMessage());
            }

            provider.failTombstoneDelete = false;
            l.onDBOpening("g3", dbDir.toString());
            assertFalse(provider.remoteFiles.containsKey("g3_DELETED"));
            assertFalse(l.hasPendingDeleteMarker("g3"));
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /**
     * Without startup hydration the DB still uploads into its prefix, so a leftover tombstone (its
     * local marker lost, e.g. with the disk) must be resolved at open, and every confirmation for
     * the purged objects dropped so they are re-uploaded before the next metadata publish.
     */
    @Test
    public void onDBOpening_withHydrationDisabled_purgesTombstonedPrefix_andClearsTracker()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-open-tombstone-no-hydration");
        Path dbDir = tmpRoot.resolve("g4");
        Files.createDirectories(dbDir);
        TombstoneTrackingProvider provider = new TombstoneTrackingProvider();
        provider.putRemoteFile("g4/000001.sst", "deleted-generation".getBytes());
        provider.putRemoteFile("g4_DELETED", "deleted".getBytes());
        CloudSyncTracker tracker = new CloudSyncTracker();
        tracker.markConfirmed("g4", dbDir.resolve("000001.sst").toString());
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(
                provider, List.of(tmpRoot.toString()), false, 0L, null, tracker);
        try {
            l.onDBOpening("g4", dbDir.toString());

            assertFalse(provider.remoteFiles.containsKey("g4/000001.sst"),
                        "stale objects of the deleted generation must be purged");
            assertFalse(provider.remoteFiles.containsKey("g4_DELETED"),
                        "the tombstone must be removed before the DB can write to the prefix");
            assertEquals(0L, tracker.confirmedCount("g4"),
                         "confirmations for purged objects must be dropped");
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /**
     * The destroy guard deletes a stale directory of a DB that was re-created at another
     * directory. That delete must purge only the stale prefix and keep the live copy's state.
     */
    @Test
    public void onDBDeleted_ofStaleDir_keepsStateOfTheSameDbLiveElsewhere() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-delete-stale-dir");
        Path staleDir = Files.createDirectories(tmpRoot.resolve("g5"));
        Path liveDir = Files.createDirectories(tmpRoot.resolve("g5_2"));
        TombstoneTrackingProvider provider = new TombstoneTrackingProvider();
        provider.putRemoteFile("g5/000001.sst", "stale".getBytes());
        provider.putRemoteFile("g5_2/000002.sst", "live".getBytes());
        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(
                provider, List.of(tmpRoot.toString()), false, 0L, null, tracker);
        try {
            l.onDBOpening("g5", staleDir.toString());
            l.onDBOpening("g5", liveDir.toString());
            tracker.markConfirmed("g5", liveDir.resolve("000002.sst").toString());

            l.onDBDeleteBegin("g5", staleDir.toString());
            l.onDBDeleted("g5", staleDir.toString());

            assertFalse(provider.remoteFiles.containsKey("g5/000001.sst"),
                        "the stale directory's prefix must be purged");
            assertTrue(provider.remoteFiles.containsKey("g5_2/000002.sst"),
                       "the live directory's prefix must not be touched");
            assertEquals(1L, tracker.confirmedCount("g5"),
                         "the live copy's confirmations must survive the stale delete");
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /**
     * The delete callback runs on a RocksDB background thread, so it must not do cloud I/O there,
     * and the deletes one compaction produces must share a single metadata publish.
     */
    @Test
    public void onTableFileDeleted_runsOffCallerThread_withOnePublishPerDbPerPass()
            throws Exception {
        CapturingProvider provider = new CapturingProvider();
        AtomicBoolean metadataHealthy = new AtomicBoolean(false);
        AtomicInteger healthyPublishes = new AtomicInteger();
        AtomicInteger failedPublishes = new AtomicInteger();
        Thread caller = Thread.currentThread();
        AtomicBoolean publishedOnCaller = new AtomicBoolean(false);
        CloudStorageEventListener l = new CloudStorageEventListener(provider, List.of(DATA_ROOT),
                false, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            List<LiveSstFile> currentLiveSstFiles(String dbName) {
                return List.of();
            }

            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                if (Thread.currentThread() == caller) {
                    publishedOnCaller.set(true);
                }
                if (!metadataHealthy.get()) {
                    failedPublishes.incrementAndGet();
                    return false;
                }
                healthyPublishes.incrementAndGet();
                return true;
            }
        };
        try {
            for (int i = 1; i <= 3; i++) {
                l.onTableFileDeleted("db0", "default", DATA_ROOT + "/db0/00000" + i + ".sst");
            }
            // Let a pass fail first: its backoff holds every later pass until all three deletes
            // are queued, so they share one publish. Flipping earlier lets a pass that saw only
            // the first delete publish on its own.
            // The healthy publish waits out the >= 1 s publish backoff, so allow slow CI headroom.
            waitForCondition(() -> failedPublishes.get() > 0, 10_000L,
                             "the first publish must fail");
            metadataHealthy.set(true);
            waitForCondition(() -> provider.deletes.size() == 3, 10_000L,
                             "all superseded SSTs must be deleted once metadata publishes");

            assertFalse(publishedOnCaller.get(), "no cloud I/O on the RocksDB callback thread");
            assertEquals(1, healthyPublishes.get(), "one metadata publish covers all deletes");
        } finally {
            l.close();
        }
    }

    @Test
    public void truncatePurgeRetry_isAbandonedOnceTheDbWritesAgain() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-truncate-retry");
        Path dbDir = tmpRoot.resolve("g3");
        Files.createDirectories(dbDir);
        Path newSst = Files.write(dbDir.resolve("000010.sst"), "new".getBytes());
        ToggleablePurgeProvider provider = new ToggleablePurgeProvider();
        provider.putRemoteFile("g3/000001.sst", "stale".getBytes());
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(
                provider, List.of(tmpRoot.toString()), false, 0L, null, new CloudSyncTracker(),
                null, 60_000L);
        try {
            provider.failPurge = true;
            l.onDBTruncated("g3", dbDir.toString());
            assertTrue(l.hasPendingTruncateMarker("g3"));

            // The DB resumes writing before the retry fires.
            l.onTableFileCreated("g3", "default", newSst.toString(), Files.size(newSst));
            provider.failPurge = false;
            provider.putRemoteFile("g3/000010.sst", "new".getBytes());

            waitForCondition(() -> !l.hasPendingTruncateMarker("g3"),
                             "abandoned retry must drop the pending-truncate marker");
            assertTrue(provider.remoteFiles.containsKey("g3/000010.sst"),
                       "A truncate purge retry must not delete objects written after the truncate");
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_completesPendingTruncatePurge_evenWithHydrationDisabled()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-open-truncate");
        Path dbDir = tmpRoot.resolve("g4");
        Files.createDirectories(dbDir);
        ToggleablePurgeProvider provider = new ToggleablePurgeProvider();
        provider.putRemoteFile("g4/000001.sst", "stale".getBytes());
        CloudStorageEventListener l =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), false);
        try {
            provider.failPurge = true;
            l.onDBTruncated("g4", dbDir.toString());
            assertTrue(l.hasPendingTruncateMarker("g4"));

            provider.failPurge = false;
            l.onDBOpening("g4", dbDir.toString());

            assertFalse(l.hasPendingTruncateMarker("g4"));
            assertFalse(provider.remoteFiles.containsKey("g4/000001.sst"));
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_sweepsStaleHydrationTempsEvenWhenHydrationIsDisabled()
            throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-hyd-sweep");
        Path dbDir = tmpRoot.resolve("g5");
        Files.createDirectories(dbDir);
        Path stale = Files.write(dbDir.resolve("000001.sst.hyd-tmp-1-123"), "partial".getBytes());
        Path keep = Files.write(dbDir.resolve("000002.sst"), "live".getBytes());
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(
                new CapturingProvider(), List.of(tmpRoot.toString()), false);
        try {
            l.onDBOpening("g5", dbDir.toString());

            assertFalse(Files.exists(stale), "stale download temp must be swept on every open");
            assertTrue(Files.exists(keep), "real files must be untouched");
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBOpening_registersLogicalName_beforeOnDBCreated() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-open-record");
        Path dbDir = tmpRoot.resolve("g6");
        Files.createDirectories(dbDir);
        AtomicReference<String> observed = new AtomicReference<>();
        CloudStorageEventListener l = new CloudStorageEventListener(new CapturingProvider(),
                List.of(tmpRoot.toString()), false, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                observed.set(dbName);
                return true;
            }

            @Override
            List<LiveSstFile> currentLiveSstFiles(String dbName) {
                return List.of();
            }
        };
        try {
            // Native events can arrive between onDBOpening and onDBCreated, with the path form.
            l.onDBOpening("logical-g6", dbDir.toString());
            l.onTableFileDeleted(dbDir.toString(), "default", dbDir.resolve("000001.sst").toString());
            l.processDeferredSstDeletes();

            assertEquals("logical-g6", observed.get());
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onCompacted_onlyMarksMetadataDirty() throws Exception {
        List<String> syncs = Collections.synchronizedList(new ArrayList<>());
        CloudStorageEventListener l = new CloudStorageEventListener(new CapturingProvider(),
                List.of(DATA_ROOT), false, 0L, null, new CloudSyncTracker(), null, 60_000L) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                syncs.add(dbName);
                return true;
            }
        };
        try {
            l.onCompacted("db-compact");
            assertTrue(syncs.isEmpty(), "compaction callback must not publish inline");
        } finally {
            l.close();
        }
        assertEquals(List.of("db-compact"), syncs, "close() must flush the dirty mark");
    }

    @Test
    public void metadataPublishFailures_backOffPerDb_andResetOnSuccess() throws Exception {
        AtomicInteger captures = new AtomicInteger();
        AtomicBoolean healthy = new AtomicBoolean(false);
        CloudStorageEventListener l = new CloudStorageEventListener(new CapturingProvider(),
                List.of(DATA_ROOT), false, 0L, null, new CloudSyncTracker(), null, 20L) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                captures.incrementAndGet();
                return healthy.get();
            }
        };
        try {
            // Exponential, capped, with a floor so tiny debounce intervals cannot hot-loop.
            assertEquals(1_000L, l.publishBackoffMs(1));
            assertEquals(2_000L, l.publishBackoffMs(2));
            assertEquals(4_000L, l.publishBackoffMs(3));
            assertEquals(CloudStorageEventListener.MAX_PUBLISH_BACKOFF_MS, l.publishBackoffMs(60));

            l.markMetadataDirty("db-bo");
            waitForCondition(() -> l.publishFailureCount("db-bo") == 1, "first failure recorded");
            // Many drain ticks (every 20ms) pass inside the 1s backoff window: no more attempts.
            LockSupport.parkNanos(500_000_000L);
            assertEquals(1, captures.get(), "a failing DB must not be retried every drain tick");

            healthy.set(true);
            waitForCondition(() -> l.publishFailureCount("db-bo") == 0 && captures.get() == 2,
                             "retry after backoff must succeed and clear the failure state");
        } finally {
            l.close();
        }
    }

    @Test
    public void newestRestorableSibling_ordersVersions_andSkipsUnrestorableDirs() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-siblings");
        CloudStorageEventListener l = CloudStorageTestFactory.newListener(
                new CapturingProvider(), List.of(tmpRoot.toString()), true);
        try {
            // A pending-delete marker for g8_9 means that directory was destroyed.
            l.onDBDeleteBegin("g8", tmpRoot.resolve("g8_9").toString());
            List<String> keys = List.of(
                    "g8/CURRENT",
                    "g8_3/CURRENT",
                    "g8_4/CURRENT",
                    "g8_4_2/CURRENT",
                    "g8_5/000001.sst",        // no CURRENT: not restorable
                    "g8_6/CURRENT", "g8_6_DELETED", // tombstoned
                    "g8_9/CURRENT",           // pending local delete
                    "g80/CURRENT",            // another DB sharing the name prefix
                    "g8_x/CURRENT");          // not a version suffix
            assertEquals("g8_4_2", l.newestRestorableSibling("", "g8", keys));
            assertEquals(null, l.newestRestorableSibling("", "g8", List.of("g8_5/000001.sst")));
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void resolveOpenPath_redirectsOnlyWithHydrationAndAWorkingListing() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-resolve-open");
        String base = tmpRoot.resolve("g9").toString();
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("g9_5/CURRENT", "MANIFEST-000005".getBytes());
        CloudStorageEventListener hydrating =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), true);
        CloudStorageEventListener notHydrating =
                CloudStorageTestFactory.newListener(provider, List.of(tmpRoot.toString()), false);
        CloudStorageEventListener listingFails = CloudStorageTestFactory.newListener(
                new CapturingProvider() {
                    @Override
                    public List<String> listFiles(String remoteDirPrefix) throws IOException {
                        throw new IOException("simulated listing failure");
                    }
                }, List.of(tmpRoot.toString()), true);
        try {
            assertEquals(tmpRoot.resolve("g9_5").toString(), hydrating.resolveOpenPath("g9", base));
            assertEquals(base, notHydrating.resolveOpenPath("g9", base));
            assertEquals(base, listingFails.resolveOpenPath("g9", base));
        } finally {
            hydrating.close();
            notHydrating.close();
            listingFails.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }

    /** Held superseded-SST deletes must not retry a failing publish faster than its backoff. */
    @Test
    public void heldSstDeletes_respectPerDbPublishBackoff() throws Exception {
        AtomicInteger captures = new AtomicInteger();
        CloudStorageEventListener l = new CloudStorageEventListener(new CapturingProvider(),
                List.of(DATA_ROOT), false, 0L, null, new CloudSyncTracker(), null, 60_000L) {
            @Override
            List<LiveSstFile> currentLiveSstFiles(String dbName) {
                return List.of();
            }

            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                captures.incrementAndGet();
                return false;
            }
        };
        try {
            l.onTableFileDeleted("db-held", "default", DATA_ROOT + "/db-held/000001.sst");
            waitForCondition(() -> l.publishFailureCount("db-held") == 1,
                             "the first failed publish must be recorded for backoff");
            // The delete retry ticks every 500 ms; the 1 s backoff must suppress those attempts.
            LockSupport.parkNanos(700_000_000L);
            assertEquals(1, captures.get(), "a held delete must not re-publish inside the backoff");
        } finally {
            l.close();
        }
    }

    /**
     * Shutdown releases every DB before the listener is closed, so a dirty DB must be published
     * from onDBClosing, while its session is still open, rather than from close().
     */
    @Test
    public void onDBClosing_publishesDirtyMetadataOnce() throws Exception {
        List<String> syncs = Collections.synchronizedList(new ArrayList<>());
        CloudStorageEventListener l = new CloudStorageEventListener(new CapturingProvider(),
                List.of(DATA_ROOT), false, 0L, null, new CloudSyncTracker(), null, 60_000L) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                syncs.add(dbName);
                return true;
            }
        };
        try {
            l.onDBClosing("db-clean");
            assertTrue(syncs.isEmpty(), "a DB with nothing dirty must not be published on close");

            l.markMetadataDirty("db-closing");
            l.onDBClosing("db-closing");
            assertEquals(List.of("db-closing"), syncs);
        } finally {
            l.close();
        }
        assertEquals(List.of("db-closing"), syncs, "close() must not publish it a second time");
    }

    @Test
    public void dirtyMark_keptWhileTruncating_butDroppedWhenDbDeleted() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-dirty-truncate");
        Path dbDir = tmpRoot.resolve("g7");
        Files.createDirectories(dbDir);
        AtomicInteger syncs = new AtomicInteger();
        CloudStorageEventListener l = new CloudStorageEventListener(new CapturingProvider(),
                List.of(tmpRoot.toString()), false, 0L, null, new CloudSyncTracker(), null,
                20L) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                syncs.incrementAndGet();
                return true;
            }
        };
        try {
            l.onDBTruncateBegin("g7", dbDir.toString());
            l.markMetadataDirty("g7");
            LockSupport.parkNanos(200_000_000L);
            assertEquals(0, syncs.get(), "no publish while the DB is truncating");

            // The DB is deleted outright: the retained mark must not publish metadata for it.
            l.onDBDeleted("g7", dbDir.toString());
            LockSupport.parkNanos(300_000_000L);
            assertEquals(0, syncs.get(), "a deleted DB's dirty mark must be dropped");
        } finally {
            l.close();
            deleteRecursively(tmpRoot.toFile());
        }
    }
}
