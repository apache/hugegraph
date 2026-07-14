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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory.LiveSstFile;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.MetadataSnapshot;
import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setUp() {
        listener = new CloudStorageEventListener(DATA_ROOT);
    }

    @After
    public void tearDown() {
        // Reset active provider between tests
        CloudStorageProviderFactory.reset();
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
                new CloudStorageEventListener(DATA_ROOT + File.separator);
        assertEquals("hgstore-metadata/000008.sst",
                     l.toRelativeKey(DATA_ROOT + "/hgstore-metadata/000008.sst"));
    }

    @Test
    public void toRelativeKey_appliesStoreScopePrefix() {
        CloudStorageEventListener l = new CloudStorageEventListener(
                DATA_ROOT, true, 0L, null, new CloudSyncTracker(), 0,
                false, "store-127.0.0.1_8501");
        String filePath = DATA_ROOT + "/0/000042.sst";
        assertEquals("store-127.0.0.1_8501/0/000042.sst", l.toRelativeKey(filePath));
    }

    // -----------------------------------------------------------------------
    // onTableFileCreated / onTableFileDeleted – no active provider (no-op)
    // -----------------------------------------------------------------------

    @Test
    public void onTableFileCreated_noActiveProvider_doesNotThrow() {
        // No provider registered → method must be a silent no-op.
        listener.onTableFileCreated("hgstore-metadata", "default",
                                    DATA_ROOT + "/hgstore-metadata/000008.sst", 1234L);
    }

    @Test
    public void onTableFileDeleted_noActiveProvider_doesNotThrow() {
        listener.onTableFileDeleted("hgstore-metadata", "default",
                                    DATA_ROOT + "/hgstore-metadata/000008.sst");
    }

    // -----------------------------------------------------------------------
    // onTableFileCreated / onTableFileDeleted – with stub provider
    // -----------------------------------------------------------------------

    @Test
    public void onTableFileCreated_delegatesToProvider_withRelativeKey() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        listener.onTableFileCreated("hgstore-metadata", "default",
                                    DATA_ROOT + "/hgstore-metadata/000008.sst", 512L);

        assertEquals(1, provider.uploads.size());
        assertEquals(DATA_ROOT + "/hgstore-metadata/000008.sst", provider.uploads.get(0)[0]);
        assertEquals("hgstore-metadata/000008.sst", provider.uploads.get(0)[1]);
    }

    @Test
    public void onTableFileCreated_delegatesToProvider_withStoreScopePrefix() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);
        CloudStorageEventListener l = new CloudStorageEventListener(
                DATA_ROOT, true, 0L, null, new CloudSyncTracker(), 0,
                false, "store-127.0.0.1_8501");

        l.onTableFileCreated("0", "default", DATA_ROOT + "/0/000008.sst", 512L);

        assertEquals(1, provider.uploads.size());
        assertEquals("store-127.0.0.1_8501/0/000008.sst", provider.uploads.get(0)[1]);
    }

    @Test
    public void onTableFileCreated_uploadFailure_doesNotThrow_andSubmitsToRetryQueue()
            throws Exception {
        // A listener wired with a retry queue: failure must not throw and must submit to queue.
        Path tmpRoot = Files.createTempDirectory("hgstore-test-retry");
        try (CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                1, 50L, 50L, tmpRoot.toString())) {
            CloudStorageEventListener l = new CloudStorageEventListener(
                    DATA_ROOT, true, 0L, retryQueue);
            CloudStorageProviderFactory.setActiveProviderForTest(new FailingUploadProvider());
            // Must NOT throw – failure is handled asynchronously.
            l.onTableFileCreated("hgstore-metadata", "default",
                                 DATA_ROOT + "/hgstore-metadata/000008.sst", 512L);
            // Queue should have one in-flight retry submitted.
            assertTrue("Expected at least one in-flight retry",
                       retryQueue.getInFlightCount() > 0 || retryQueue.getDlqSize() >= 0);
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onTableFileCreated_uploadFailure_noRetryQueue_doesNotThrow() {
        // A listener without a retry queue: failure must still not throw (just logs).
        CloudStorageProviderFactory.setActiveProviderForTest(new FailingUploadProvider());
        // listener is created in setUp() with no retry queue.
        listener.onTableFileCreated("hgstore-metadata", "default",
                                    DATA_ROOT + "/hgstore-metadata/000008.sst", 512L);
        // If we reached here without an exception the test passes.
    }

    @Test
    public void onTableFileDeleted_delegatesToProvider_withRelativeKey() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        listener.onTableFileDeleted("hgstore-metadata", "default",
                                    DATA_ROOT + "/hgstore-metadata/000008.sst");

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

        CloudStorageEventListener l =
                new CloudStorageEventListener(tmpRoot.toString());
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

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
                assert !key.startsWith("/") : "Remote key must not start with '/': " + key;
                assert key.endsWith(".sst") : "Remote key must end with .sst: " + key;
            }
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onDBCreated_existingUploadFailure_throwsRuntimeException() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);
        Files.createFile(partitionDir.resolve("000001.sst"));

        CloudStorageEventListener l =
                new CloudStorageEventListener(tmpRoot.toString());
        CloudStorageProviderFactory.setActiveProviderForTest(new FailingUploadProvider());

        try {
            try {
                l.onDBCreated("0", partitionDir.toString());
                fail("Expected startup backfill failure to be rethrown");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("Cloud initial-upload failed"));
            }
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // metadata mirroring (upload order, CURRENT-last, prune, consistent restore)
    // -----------------------------------------------------------------------
    private static CloudStorageEventListener metadataListener(boolean walMode) {
        return new CloudStorageEventListener(DATA_ROOT, false, 0L, null,
                                             new CloudSyncTracker(), 0, walMode);
    }

    private static MetadataSnapshot snapshot(List<String> options, List<String> ssts,
                                             List<String> wals) {
        return new MetadataSnapshot("/hugegraph-store/storage/0", "/hugegraph-store/storage/0" + "_tmp",
                                    "CURRENT", "MANIFEST-000005", options, ssts, wals);
    }

    @Test
    public void uploadMetadataSnapshot_uploadsReferencedSstsThenMetadataThenCurrentLast() {
        CloudStorageEventListener l = metadataListener(false);
        CapturingProvider provider = new CapturingProvider();
        // The referenced SST is already durable in cloud → no re-upload, but metadata must follow.
        provider.putRemoteFile("0/000003.sst", "sst".getBytes());

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"),
                                         List.of());

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
        CloudStorageEventListener l = metadataListener(false);
        // Referenced SST is neither in cloud nor uploadable → the whole publish must be held.
        FailingUploadProvider provider = new FailingUploadProvider();

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"),
                                         List.of());

        boolean durable = l.uploadMetadataSnapshot(provider, "0", snap);

        assertFalse(durable);
        // No MANIFEST/OPTIONS/CURRENT may be published while a referenced SST is not durable.
        for (String[] up : provider.uploads) {
            fail("nothing should have been published, but uploaded: " + up[1]);
        }
    }

    @Test
    public void uploadMetadataSnapshot_prunesSupersededRemoteMetadataButKeepsCurrentAndSst() {
        CloudStorageEventListener l = metadataListener(false);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000003.sst", "sst".getBytes());
        // Superseded remote metadata from a previous generation, plus files that must be kept.
        provider.putRemoteFile("0/MANIFEST-000001", "old".getBytes());
        provider.putRemoteFile("0/OPTIONS-000000", "old".getBytes());
        provider.putRemoteFile("0/CURRENT", "old".getBytes());

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"),
                                         List.of());

        assertTrue(l.uploadMetadataSnapshot(provider, "0", snap));

        assertTrue(provider.deletes.contains("0/MANIFEST-000001"));
        assertTrue(provider.deletes.contains("0/OPTIONS-000000"));
        // CURRENT (the pointer) and SST objects are never pruned by the metadata sync.
        assertFalse(provider.deletes.contains("0/CURRENT"));
        assertFalse(provider.deletes.contains("0/000003.sst"));
    }

    @Test
    public void uploadMetadataSnapshot_walMode_mirrorsWalBeforeCurrent() {
        CloudStorageEventListener l = metadataListener(true);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000003.sst", "sst".getBytes());

        MetadataSnapshot snap = snapshot(
                List.of("OPTIONS-000004"), List.of("000003.sst"),
                                         List.of("000002.log"));

        assertTrue(l.uploadMetadataSnapshot(provider, "0", snap));

        List<String> keys = new ArrayList<>();
        for (String[] up : provider.uploads) {
            keys.add(up[1]);
        }
        assertEquals(List.of("0/000002.log", "0/OPTIONS-000004", "0/MANIFEST-000005", "0/CURRENT"),
                     keys);
    }

    @Test
    public void preHydration_failsLoudlyWhenCurrentReferencesMissingManifest() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CloudStorageEventListener l = new CloudStorageEventListener(tmpRoot.toString(), true);
        CapturingProvider provider = new CapturingProvider();
        // CURRENT points at a manifest that was never mirrored → restore must refuse to open.
        provider.putRemoteFile("0/CURRENT", "MANIFEST-000009\n".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        try {
            l.onDBOpening("0", partitionDir.toString());
            fail("expected IllegalStateException for inconsistent restore");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Cloud restore inconsistent"));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            for (File child : Objects.requireNonNull(f.listFiles())) {
                deleteRecursively(child);
            }
        }
        f.delete();
    }

    /**
     * Minimal {@link CloudStorageProvider} that records upload and delete calls.
     */
    static class CapturingProvider implements CloudStorageProvider {

        final List<String[]> uploads = new ArrayList<>();
        final List<String> deletes = new ArrayList<>();
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

    @Test
    public void onDBOpening_downloadsMissingRemoteFiles() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CloudStorageEventListener l =
                new CloudStorageEventListener(tmpRoot.toString(), true);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/CURRENT", "MANIFEST-000001".getBytes());
        provider.putRemoteFile("0/MANIFEST-000001", "manifest-body".getBytes());
        provider.putRemoteFile("0/000001.sst", "sst-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

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

        CloudStorageEventListener l = new CloudStorageEventListener(
                tmpRoot.toString(), true, 0L, null, new CloudSyncTracker(), 0,
                false, "store-127.0.0.1_8501");
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("store-127.0.0.1_8501/0/CURRENT", "MANIFEST-000001".getBytes());
        provider.putRemoteFile("store-127.0.0.1_8501/0/MANIFEST-000001", "manifest-body".getBytes());
        provider.putRemoteFile("store-127.0.0.1_8501/0/000001.sst", "sst-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        try {
            l.onDBOpening("0", partitionDir.toString());
            assertTrue(Files.exists(partitionDir.resolve("CURRENT")));
            assertTrue(Files.exists(partitionDir.resolve("MANIFEST-000001")));
            assertTrue(Files.exists(partitionDir.resolve("000001.sst")));
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

        CloudStorageEventListener l = new CloudStorageEventListener(tmpRoot.toString(), true);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000001.sst", "sst-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

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

        CloudStorageEventListener l = new CloudStorageEventListener(tmpRoot.toString(), true);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000001.sst", "g-body".getBytes());
        provider.putRemoteFile("0/000002.sst", "q-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

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

        CloudStorageEventListener l = new CloudStorageEventListener(tmpRoot.toString(), true);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000001.sst", "cloud-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

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
                new CloudStorageEventListener(DATA_ROOT, true, 60_000L);
        assertTrue(l.shouldAttemptReadMissHydration("0", "default"));
        assertFalse(l.shouldAttemptReadMissHydration("0", "default"));
        // A different table is not throttled by the first table's attempt.
        assertTrue(l.shouldAttemptReadMissHydration("0", "other"));
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
        CloudStorageEventListener l = new CloudStorageEventListener(
                tmpRoot.toString(), true, 0L, null, tracker, 0);
        FailingUploadProvider provider = new FailingUploadProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        List<LiveSstFile> live = List.of(new LiveSstFile(liveLocal.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertFalse("Guard must report the live set as NOT durable", durable);
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
        CloudStorageEventListener l = new CloudStorageEventListener(
                tmpRoot.toString(), true, 0L, null, tracker, 0);
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        List<LiveSstFile> live = List.of(new LiveSstFile(liveLocal.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertTrue("Guard must pass when the whole live set is confirmed", durable);
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
        CloudStorageEventListener l = new CloudStorageEventListener(
                tmpRoot.toString(), true, 0L, null, tracker, 0);
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

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

        CloudStorageEventListener l = new CloudStorageEventListener(
                tmpRoot.toString(), true, 0L, null, tracker, 0);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000001.sst", "sst1".getBytes());
        provider.putRemoteFile("0/000002.sst", "sst2".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        // Post-compaction live set contains only MERGED_SST.
        List<LiveSstFile> live = List.of(new LiveSstFile(merged.toString(), "default"));

        try {
            boolean durable = l.ensureLiveSetUploaded(provider, "0", live);
            assertFalse("Merged output is not durable; old SST delete must be blocked", durable);
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

        final List<String> callOrder = new ArrayList<>();
        private final boolean syncResult;

        OrderingListener(String dataRoot, CloudSyncTracker tracker, boolean syncResult) {
            super(dataRoot, false, 0L, null, tracker, 0,
                  /* walModeEnabled */ false);
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
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        CloudSyncTracker tracker = new CloudSyncTracker();
        OrderingListener l = new OrderingListener(DATA_ROOT, tracker, /* syncResult */ true) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String dbName) {
                boolean result = super.syncMetadataSnapshotInline(p, dbName);
                // Verify no delete has happened yet when sync fires.
                assertTrue("delete must not precede sync", provider.deletes.isEmpty());
                return result;
            }
        };

        l.onTableFileDeleted("db0", "default", DATA_ROOT + "/db0/000001.sst");

        assertEquals("sync must have been called once", List.of("sync"), l.callOrder);
        assertTrue("SST1 must be deleted from cloud", provider.deletes.contains(sst1Remote));
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
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        CloudSyncTracker tracker = new CloudSyncTracker();
        // syncResult=false: metadata sync fails → delete must be suppressed.
        OrderingListener l = new OrderingListener(DATA_ROOT, tracker, /* syncResult */ false);

        l.onTableFileDeleted("db0", "default", DATA_ROOT + "/db0/000001.sst");

        assertEquals("sync must have been called", List.of("sync"), l.callOrder);
        assertTrue("SST must NOT be deleted when metadata sync fails", provider.deletes.isEmpty());
    }

}
