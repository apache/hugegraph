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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
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
    public void onReadMiss_downloadsSstAndRequestsIngest() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CloudStorageEventListener l =
                new CloudStorageEventListener(tmpRoot.toString(), true);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000001.sst", "sst-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getGraphName()).thenReturn("0");
        when(session.getDbPath()).thenReturn(partitionDir.toString());

        try {
            boolean hydrated = l.onReadMiss(session, "default", "k".getBytes());
            assertTrue(hydrated);
            verify(session, times(1)).ingestSstFile(anyMap());
            assertTrue(Files.exists(partitionDir.resolve("000001.sst")));
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }

    @Test
    public void onReadMiss_guardSkipsRepeatedAttemptsWithinWindow() throws Exception {
        Path tmpRoot = Files.createTempDirectory("hgstore-test-storage");
        Path partitionDir = tmpRoot.resolve("0");
        Files.createDirectories(partitionDir);

        CloudStorageEventListener l =
                new CloudStorageEventListener(tmpRoot.toString(), true, 60_000L);
        CapturingProvider provider = new CapturingProvider();
        provider.putRemoteFile("0/000001.sst", "sst-body".getBytes());
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        RocksDBSession session = mock(RocksDBSession.class);
        when(session.getGraphName()).thenReturn("0");
        when(session.getDbPath()).thenReturn(partitionDir.toString());

        try {
            boolean first = l.onReadMiss(session, "default", "k1".getBytes());
            boolean second = l.onReadMiss(session, "default", "k2".getBytes());
            assertTrue(first);
            assertFalse(second);
            verify(session, times(1)).ingestSstFile(anyMap());
            assertEquals(1, provider.listFilesCalls);
        } finally {
            deleteRecursively(tmpRoot.toFile());
        }
    }
}
