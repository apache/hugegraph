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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link CloudUploadRetryQueue}.
 */
@SuppressWarnings("BusyWait")
public class CloudUploadRetryQueueTest {

    private Path tmpRoot;

    @Before
    public void setUp() throws IOException {
        tmpRoot = Files.createTempDirectory("hgstore-retry-queue-test");
        CloudStorageProviderFactory.reset();
    }

    @After
    public void tearDown() {
        CloudStorageProviderFactory.reset();
        deleteRecursively(tmpRoot.toFile());
    }

    // -----------------------------------------------------------------------
    // submit → retry succeeds on second attempt
    // -----------------------------------------------------------------------

    @Test
    public void submit_retriesAndSucceeds() throws Exception {
        CountDownLatch successLatch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);

        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                if (callCount.incrementAndGet() == 1) {
                    throw new IOException("transient failure");
                }
                // Second call succeeds.
                successLatch.countDown();
            }
        });

        // Create a real SST file so the retry doesn't drop it as "compacted".
        Path sstFile = tmpRoot.resolve("000001.sst");
        Files.createFile(sstFile);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(3, 50L, 200L, tmpRoot.toString())) {

            queue.submit("db1", "default", sstFile.toString(),
                         "db1/000001.sst", new IOException("initial failure"));

            assertTrue("Upload should have succeeded within 2 s",
                       successLatch.await(2, TimeUnit.SECONDS));
            assertEquals(0, queue.getDlqSize());
        }
    }

    // -----------------------------------------------------------------------
    // submit → all attempts fail → moved to DLQ
    // -----------------------------------------------------------------------

    @Test
    public void submit_exhaustsRetriesAndMovesToDlq() throws Exception {
        AtomicInteger uploadCalls = new AtomicInteger(0);

        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                uploadCalls.incrementAndGet();
                throw new IOException("permanent failure");
            }
        });

        Path sstFile = tmpRoot.resolve("000002.sst");
        Files.createFile(sstFile);

        // maxAttempts=2, initial+retry delay 50ms → DLQ after ~100ms total
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(2, 50L, 50L, tmpRoot.toString())) {

            queue.submit("db1", "default", sstFile.toString(),
                         "db1/000002.sst", new IOException("initial"));

            // Wait for the retry cycle to finish.
            long deadline = System.currentTimeMillis() + 3_000L;
            while (queue.getDlqSize() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            assertEquals("Task should be in the DLQ after max retries", 1, queue.getDlqSize());
            FailedUploadTask entry = queue.getDlqEntries().get(0);
            assertEquals("db1", entry.getDbName());
            assertEquals("db1/000002.sst", entry.getRemoteKey());
            assertTrue(entry.getAttemptCount() >= 1);
        }
    }

    // -----------------------------------------------------------------------
    // DLQ persistence – entries survive queue reconstruction
    // -----------------------------------------------------------------------

    @Test
    public void dlq_persistsAndLoadsFromDisk() throws Exception {
        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                throw new IOException("always fails");
            }
        });

        Path sstFile = tmpRoot.resolve("000003.sst");
        Files.createFile(sstFile);

        // First queue instance: run until task hits DLQ.
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(1, 50L, 50L, tmpRoot.toString())) {
            queue.submit("db2", "cf1", sstFile.toString(),
                         "db2/000003.sst", new IOException("fail"));

            long deadline = System.currentTimeMillis() + 3_000L;
            while (queue.getDlqSize() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(1, queue.getDlqSize());
        }

        // Verify the DLQ file was written.
        assertTrue("DLQ file should exist",
                   Files.exists(tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME)));

        // Second queue instance: should load the persisted entry.
        try (CloudUploadRetryQueue queue2 =
                     new CloudUploadRetryQueue(1, 50L, 50L, tmpRoot.toString())) {
            assertEquals("DLQ entry should have been loaded from disk", 1, queue2.getDlqSize());
            FailedUploadTask loaded = queue2.getDlqEntries().get(0);
            assertEquals("db2", loaded.getDbName());
            assertEquals("cf1", loaded.getCfName());
            assertEquals("db2/000003.sst", loaded.getRemoteKey());
        }
    }

    // -----------------------------------------------------------------------
    // replayDlq – successful replay clears the DLQ
    // -----------------------------------------------------------------------

    @Test
    public void replayDlq_successfulUploadClearsDlq() throws Exception {
        // Step 1: force a task into the DLQ.
        AtomicInteger uploadCalls = new AtomicInteger(0);
        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                if (uploadCalls.incrementAndGet() <= 1) {
                    throw new IOException("fail during initial + retry");
                }
                // Subsequent calls succeed (replay).
            }
        });

        Path sstFile = tmpRoot.resolve("000004.sst");
        Files.createFile(sstFile);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(1, 50L, 50L, tmpRoot.toString())) {
            queue.submit("db3", "default", sstFile.toString(),
                         "db3/000004.sst", new IOException("initial fail"));

            // Wait for DLQ.
            long deadline = System.currentTimeMillis() + 3_000L;
            while (queue.getDlqSize() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(1, queue.getDlqSize());

            // Now replay: the provider will succeed.
            queue.replayDlq();

            assertEquals("DLQ should be empty after successful replay", 0, queue.getDlqSize());
        }
    }

    // -----------------------------------------------------------------------
    // replayDlq – local file gone → dropped silently, not re-queued
    // -----------------------------------------------------------------------

    @Test
    public void replayDlq_dropsEntryWhenLocalFileGone() throws Exception {
        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                throw new IOException("fail");
            }
        });

        // File path that does NOT exist (deleted SST).
        String nonExistentFile = tmpRoot.resolve("gone.sst").toString();

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(1, 50L, 50L, tmpRoot.toString())) {
            // Submit; the retry will run but the local file doesn't exist → drop silently.
            queue.submit("db4", "default", nonExistentFile,
                         "db4/gone.sst", new IOException("initial"));

            // Wait for the retry to drain (file-not-found path → task dropped).
            long deadline = System.currentTimeMillis() + 3_000L;
            while (queue.getInFlightCount() > 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            // Task dropped (not in DLQ) because local file was gone.
            assertEquals("Task with non-existent file should be silently dropped",
                         0, queue.getDlqSize());
        }
    }

    // -----------------------------------------------------------------------
    // Serialisation round-trip
    // -----------------------------------------------------------------------

    @Test
    public void serialize_deserialize_roundTrip() {
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(3, 100L, 1000L, tmpRoot.toString())) {

            FailedUploadTask original = new FailedUploadTask(
                    "db\twith-tab", "cf-name", "/data/root/file.sst",
                    "data/root/file.sst", 1234567890123L, 3,
                    "Error with\nnewline and\\backslash");

            String line = queue.serialize(original);
            assertFalse("Serialised line must not contain raw tab in field values",
                        hasUnescapedTab(line));

            FailedUploadTask restored = queue.deserialize(line);
            assertNotNull(restored);
            assertEquals(original.getDbName(), restored.getDbName());
            assertEquals(original.getCfName(), restored.getCfName());
            assertEquals(original.getFilePath(), restored.getFilePath());
            assertEquals(original.getRemoteKey(), restored.getRemoteKey());
            assertEquals(original.getFailedAt(), restored.getFailedAt());
            assertEquals(original.getAttemptCount(), restored.getAttemptCount());
            assertEquals(original.getLastError(), restored.getLastError());
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Returns true if the serialised line has the wrong number of tab-separated fields. */
    private boolean hasUnescapedTab(String serialised) {
        String[] parts = serialised.split("\t", -1);
        return parts.length != 7;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteRecursively(java.io.File f) {
        if (f.isDirectory()) {
            for (java.io.File child : Objects.requireNonNull(f.listFiles())) {
                deleteRecursively(child);
            }
        }
        f.delete();
    }

    // -----------------------------------------------------------------------
    // Stub providers
    // -----------------------------------------------------------------------

    static class CapturingProvider implements CloudStorageProvider {

        final List<String[]> uploads = new ArrayList<>();

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

        @Override
        public void deleteFile(String remoteKey) {
        }

        @Override
        public boolean fileExists(String remoteKey) {
            return false;
        }

        @Override
        public void downloadFile(String remoteKey, String localPath) {
        }

        @Override
        public void close() {
        }
    }
}









