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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CloudUploadRetryQueue}.
 */
@SuppressWarnings({"BusyWait", "resource"})
public class CloudUploadRetryQueueTest {

    private Path tmpRoot;

    @BeforeEach
    public void setUp() throws IOException {
        tmpRoot = Files.createTempDirectory("hgstore-retry-queue-test");
    }

    @AfterEach
    public void tearDown() {
        deleteRecursively(tmpRoot.toFile());
    }

    @Test
    public void constructor_rejectsNullProvider() {
        try {
            CloudStorageTestFactory.newRetryQueue(null, 3, 50L, 200L);
            fail("Expected NullPointerException for null provider");
        } catch (NullPointerException expected) {
            assertTrue(String.valueOf(expected.getMessage()).contains("provider"),
                       "Message should mention provider: " + expected.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // submit → retry succeeds on second attempt
    // -----------------------------------------------------------------------

    @Test
    public void submit_retriesAndSucceeds() throws Exception {
        CountDownLatch successLatch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);

        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                if (callCount.incrementAndGet() == 1) {
                    throw new IOException("transient failure");
                }
                // Second call succeeds.
                successLatch.countDown();
            }
        };

        // Create a real SST file so the retry doesn't drop it as "compacted".
        Path sstFile = tmpRoot.resolve("000001.sst");
        Files.createFile(sstFile);

        try (CloudUploadRetryQueue queue =
                     CloudStorageTestFactory.newRetryQueue(provider, 3, 50L, 200L)) {

            queue.submit("db1", "default", sstFile.toString(),
                         "db1/000001.sst", new IOException("initial failure"));

            assertTrue(successLatch.await(2, TimeUnit.SECONDS),
                       "Upload should have succeeded within 2 s");
            assertEquals(0L, queue.getGivenUpCount());
        }
    }

    @Test
    public void submit_callbackFailureAfterSuccessfulUpload_doesNotMisclassifyAsUploadFailure()
            throws Exception {
        Path sstFile = tmpRoot.resolve("000001-callback-fail.sst");
        Files.createFile(sstFile);

        CapturingProvider provider = new CapturingProvider();

        try (CloudUploadRetryQueue queue = CloudStorageTestFactory.newRetryQueue(provider, 
                1, 50L, 50L,
                (db, source, epoch) -> {
                    throw new RuntimeException("tracker callback failed");
                })) {

            queue.submit("db-cb", "default", sstFile.toString(),
                         "db-cb/000001.sst", new IOException("initial failure"));

            long deadline = System.currentTimeMillis() + 3_000L;
            while (provider.uploads.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }

            assertFalse(provider.uploads.isEmpty(),
                        "Upload should still succeed even if callback throws");
            assertEquals(0L,
                         queue.getGivenUpCount(),
                         "Callback failure after successful upload must not be given up");
        }
    }

    // -----------------------------------------------------------------------
    // submit → all attempts fail → given up
    // -----------------------------------------------------------------------

    @Test
    public void submit_exhaustsRetriesAndGivesUp() throws Exception {
        AtomicInteger uploadCalls = new AtomicInteger(0);

        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                uploadCalls.incrementAndGet();
                throw new IOException("permanent failure");
            }
        };

        Path sstFile = tmpRoot.resolve("000002.sst");
        Files.createFile(sstFile);

        // maxAttempts=2, initial+retry delay 50ms → given up after ~100ms total
        try (CloudUploadRetryQueue queue =
                     CloudStorageTestFactory.newRetryQueue(provider, 2, 50L, 50L)) {

            queue.submit("db1", "default", sstFile.toString(),
                         "db1/000002.sst", new IOException("initial"));

            // Wait for the retry cycle to finish.
            long deadline = System.currentTimeMillis() + 3_000L;
            while (queue.getGivenUpCount() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            assertEquals(1L, queue.getGivenUpCount(),
                         "Task should be given up after max retries");
            assertEquals(2, uploadCalls.get(),
                         "Provider must be called once per whole-file retry (maxAttempts=2)");
        }
    }

    // -----------------------------------------------------------------------
    // Rejection safety – submit after scheduler shutdown must not throw
    // -----------------------------------------------------------------------

    @Test
    public void submit_afterShutdown_doesNotThrow_andGivesUp() throws Exception {
        AtomicInteger uploadCalls = new AtomicInteger(0);
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                uploadCalls.incrementAndGet();
                throw new IOException("should never run – scheduler is shut down");
            }
        };

        Path sstFile = tmpRoot.resolve("000009.sst");
        Files.createFile(sstFile);

        // maxAttempts > 0 so submit() routes through scheduleRetry(), which will attempt to
        // schedule on the executor. After close() the scheduler is shut down and
        // scheduler.schedule(...) throws RejectedExecutionException.
        CloudUploadRetryQueue queue =
                CloudStorageTestFactory.newRetryQueue(provider, 3, 50L, 200L);
        queue.close();

        // This simulates a RocksDB callback firing during a shutdown race. It must NOT throw
        // (which would leak across the JNI/event boundary) and must give up on the upload.
        try {
            queue.submit("db-shutdown", "default", sstFile.toString(),
                         "db-shutdown/000009.sst", new IOException("initial upload failed"));
        } catch (Exception e) {
            fail("submit() must not throw when scheduler is shut down; caught: " + e);
        }

        assertEquals(1L, queue.getGivenUpCount(), "Rejected retry must be counted as given up");
        assertEquals(0, uploadCalls.get(), "No retry may run once the scheduler is shut down");
    }

    @Test
    public void close_givesUpOnPendingScheduledRetries() throws Exception {
        // Provider always fails, so the first attempt schedules a retry. A long initial delay keeps
        // that retry PENDING (scheduled, not yet run) so close()'s forced shutdown must give up on
        // it explicitly (counted + logged) instead of silently dropping the upload.
        AtomicInteger uploadCalls = new AtomicInteger(0);
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                uploadCalls.incrementAndGet();
                throw new IOException("always fails");
            }
        };

        Path sstFile = tmpRoot.resolve("000042.sst");
        Files.createFile(sstFile);

        CloudUploadRetryQueue queue =
                CloudStorageTestFactory.newRetryQueue(provider, 3, 60_000L, 120_000L);
        // First failure schedules attempt #1 ~60s out — it will still be pending at close().
        queue.submit("db-x", "default", sstFile.toString(), "db-x/000042.sst",
                     new IOException("initial failure"));

        // Let the submit schedule the retry.
        long deadline = System.currentTimeMillis() + 2_000L;
        while (queue.getInFlightCount() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(0L,
                     queue.getGivenUpCount(),
                     "Retry must be scheduled (pending), not yet given up");

        // close() force-stops the scheduler after the grace period; the pending retry must be
        // given up on.
        queue.close();

        assertEquals(1L,
                     queue.getGivenUpCount(),
                     "Pending scheduled retry must be given up on close");
        assertEquals(0, uploadCalls.get(), "The pending retry must never have run");
    }

    @Test
    public void close_givesUpOnInFlightHungRetry() throws Exception {
        // A retry that has STARTED but is hung in a slow/unresponsive provider.uploadFile(...) at
        // shutdown must still be given up on explicitly (not just scheduled-but-unrun retries),
        // so its staging pin is released and the loss is counted rather than silent.
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean uploadStarted = new AtomicBoolean(false);
        CapturingProvider provider = new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) {
                uploadStarted.set(true);
                // Block UNINTERRUPTIBLY, simulating cloud I/O that ignores the shutdown interrupt.
                boolean done = false;
                while (!done) {
                    try {
                        release.await();
                        done = true;
                    } catch (InterruptedException ie) {
                        // swallow — keep hanging so the retry is genuinely stuck in-flight
                    }
                }
            }
        };

        Path sstFile = tmpRoot.resolve("000077.sst");
        Files.createFile(sstFile);

        CloudUploadRetryQueue queue =
                CloudStorageTestFactory.newRetryQueue(provider, 3, 50L, 200L);
        // The scheduled retry starts quickly and then hangs inside uploadFile.
        queue.submit("db-h", "default", sstFile.toString(), "db-h/000077.sst",
                     new IOException("initial failure"));

        long deadline = System.currentTimeMillis() + 3_000L;
        while (!uploadStarted.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(uploadStarted.get(), "Retry upload must have started (in-flight)");
        assertEquals(0L, queue.getGivenUpCount(), "Nothing given up before shutdown");

        try {
            // Force a fast shutdown; the in-flight (hung) retry must be given up on.
            queue.close(200, TimeUnit.MILLISECONDS);

            assertEquals(1L,
                         queue.getGivenUpCount(),
                         "In-flight hung retry must be given up on forced shutdown");
        } finally {
            release.countDown(); // let the hung thread exit
        }
    }

    @Test
    public void giveUp_releasesStagingPinButKeepsSourceSst() throws Exception {
        Path source = Files.writeString(tmpRoot.resolve("000007.sst"), "sst");
        Path pin = Files.createLink(tmpRoot.resolve("000007.sst.upload-1"), source);
        // maxAttempts=0 gives up on the task immediately.
        try (CloudUploadRetryQueue queue =
                     CloudStorageTestFactory.newRetryQueue(new CapturingProvider(), 0,
                                                           50L, 50L)) {
            queue.submitPinned("db", "cf", pin.toString(), source.toString(), "db/000007.sst",
                               1L, new IOException("outage"));

            assertEquals(1L, queue.getGivenUpCount());
            assertFalse(Files.exists(pin), "give-up must release the staging hard-link");
            assertTrue(Files.exists(source), "give-up must not touch the source SST");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

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









