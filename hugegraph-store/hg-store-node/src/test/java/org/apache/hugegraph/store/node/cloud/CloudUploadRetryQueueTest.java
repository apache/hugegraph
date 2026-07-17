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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link CloudUploadRetryQueue}.
 */
@SuppressWarnings({"BusyWait", "resource"})
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

    @Test
    public void submit_transientNullProvider_reschedulesAndEventuallySucceeds() throws Exception {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(null);

        Path sstFile = tmpRoot.resolve("000001-null-provider.sst");
        Files.createFile(sstFile);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(4, 100L, 400L, tmpRoot.toString())) {

            queue.submit("db-null", "default", sstFile.toString(),
                         "db-null/000001.sst", new IOException("initial failure"));

            Thread restorer = new Thread(() -> {
                try {
                    Thread.sleep(350L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                CloudStorageProviderFactory.setActiveProviderForTest(provider);
            });
            restorer.setDaemon(true);
            restorer.start();

            long deadline = System.currentTimeMillis() + 6_000L;
            while (provider.uploads.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            restorer.join(2_000L);

            assertFalse("Retry must recover automatically after provider returns",
                        provider.uploads.isEmpty());
            assertEquals("Recovered retry must not be moved to DLQ", 0, queue.getDlqSize());
        }
    }

    @Test
    public void submit_providerOutageLongerThanRetryBudget_stillRetriesAfterRecovery()
            throws Exception {
        // Regression guard: provider-unavailable windows must NOT consume upload retry attempts.
        // With maxAttempts=2 and delay=100ms, an old implementation would DLQ in ~200ms.
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(null);

        Path sstFile = tmpRoot.resolve("000001-long-outage.sst");
        Files.createFile(sstFile);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(2, 100L, 100L, tmpRoot.toString())) {

            queue.submit("db-long", "default", sstFile.toString(),
                         "db-long/000001.sst", new IOException("initial failure"));

            // Keep outage well beyond the old retry budget window.
            Thread restorer = new Thread(() -> {
                try {
                    Thread.sleep(900L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                CloudStorageProviderFactory.setActiveProviderForTest(provider);
            });
            restorer.setDaemon(true);
            restorer.start();

            // Before recovery the task must not be prematurely DLQed.
            Thread.sleep(450L);
            assertEquals("Provider-unavailable retries must not consume budget and DLQ early",
                         0, queue.getDlqSize());

            long deadline = System.currentTimeMillis() + 6_000L;
            while (provider.uploads.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            restorer.join(2_000L);

            assertFalse("Retry must still run after a long outage once provider returns",
                        provider.uploads.isEmpty());
            assertEquals("Recovered retry must not be moved to DLQ", 0, queue.getDlqSize());
        }
    }

    @Test
    public void submit_providerPermanentlyUnavailable_eventuallyMovesToDlq() throws Exception {
        CloudStorageProviderFactory.setActiveProviderForTest(null);

        Path sstFile = tmpRoot.resolve("000001-permanent-outage.sst");
        Files.createFile(sstFile);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(2, 100L, 100L, tmpRoot.toString())) {
            queue.setMaxProviderUnavailableRetriesForTest(3);
            queue.setProviderUnavailableMaxDelayMsForTest(120L);

            queue.submit("db-perm", "default", sstFile.toString(),
                         "db-perm/000001.sst", new IOException("initial failure"));

            long deadline = System.currentTimeMillis() + 5_000L;
            while (queue.getDlqSize() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }

            assertEquals("Permanent provider outage must eventually surface as DLQ",
                         1, queue.getDlqSize());
            FailedUploadTask task = queue.getDlqEntries().get(0);
            assertTrue("DLQ reason should indicate provider-unavailable exhaustion",
                       task.getLastError().contains("No active provider"));
        }
    }

    @Test
    public void submit_callbackFailureAfterSuccessfulUpload_doesNotMisclassifyAsUploadFailure()
            throws Exception {
        Path sstFile = tmpRoot.resolve("000001-callback-fail.sst");
        Files.createFile(sstFile);

        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        try (CloudUploadRetryQueue queue = new CloudUploadRetryQueue(
                1, 50L, 50L, tmpRoot.toString(),
                (db, source, epoch) -> {
                    throw new RuntimeException("tracker callback failed");
                })) {

            queue.submit("db-cb", "default", sstFile.toString(),
                         "db-cb/000001.sst", new IOException("initial failure"));

            long deadline = System.currentTimeMillis() + 3_000L;
            while (provider.uploads.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }

            assertFalse("Upload should still succeed even if callback throws",
                        provider.uploads.isEmpty());
            assertEquals("Callback failure after successful upload must not DLQ",
                         0, queue.getDlqSize());
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
    // Rejection safety – submit after scheduler shutdown must not throw
    // -----------------------------------------------------------------------

    @Test
    public void submit_afterShutdown_doesNotThrow_andPersistsToDlq() throws Exception {
        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                throw new IOException("should never run – scheduler is shut down");
            }
        });

        Path sstFile = tmpRoot.resolve("000009.sst");
        Files.createFile(sstFile);

        // maxAttempts > 0 so submit() routes through scheduleRetry(), which will attempt to
        // schedule on the executor. After close() the scheduler is shut down and
        // scheduler.schedule(...) throws RejectedExecutionException.
        CloudUploadRetryQueue queue =
                new CloudUploadRetryQueue(3, 50L, 200L, tmpRoot.toString());
        queue.close();

        // This simulates a RocksDB callback firing during a shutdown race. It must NOT throw
        // (which would leak across the JNI/event boundary) and must persist the retry intent.
        try {
            queue.submit("db-shutdown", "default", sstFile.toString(),
                         "db-shutdown/000009.sst", new IOException("initial upload failed"));
        } catch (Exception e) {
            org.junit.Assert.fail("submit() must not throw when scheduler is shut down; caught: "
                                  + e);
        }

        assertEquals("Rejected retry must be persisted to the DLQ", 1, queue.getDlqSize());
        FailedUploadTask entry = queue.getDlqEntries().get(0);
        assertEquals("db-shutdown", entry.getDbName());
        assertEquals("db-shutdown/000009.sst", entry.getRemoteKey());
        assertTrue("DLQ entry should record the rejection cause",
                   entry.getLastError().contains("rejected"));
    }

    @Test
    public void close_movesPendingScheduledRetriesToDlq() throws Exception {
        // Provider always fails, so the first attempt schedules a retry. A long initial delay keeps
        // that retry PENDING (scheduled, not yet run) so close()'s forced shutdown must rescue it
        // into the DLQ instead of silently dropping the only remaining upload path.
        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                throw new IOException("always fails");
            }
        });

        Path sstFile = tmpRoot.resolve("000042.sst");
        Files.createFile(sstFile);

        CloudUploadRetryQueue queue =
                new CloudUploadRetryQueue(3, 60_000L, 120_000L, tmpRoot.toString());
        // First failure schedules attempt #1 ~60s out — it will still be pending at close().
        queue.submit("db-x", "default", sstFile.toString(), "db-x/000042.sst",
                     new IOException("initial failure"));

        // Let the submit schedule the retry.
        long deadline = System.currentTimeMillis() + 2_000L;
        while (queue.getInFlightCount() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals("Retry must be scheduled (pending), not yet in the DLQ", 0, queue.getDlqSize());

        // close() force-stops the scheduler after the grace period; the pending retry must be
        // rescued into the DLQ.
        queue.close();

        assertEquals("Pending scheduled retry must be moved to the DLQ on close",
                     1, queue.getDlqSize());
        FailedUploadTask entry = queue.getDlqEntries().get(0);
        assertEquals("db-x", entry.getDbName());
        assertEquals("db-x/000042.sst", entry.getRemoteKey());
        assertTrue("DLQ entry should record the shutdown reason",
                   entry.getLastError().contains("shutdown"));
    }

    @Test
    public void close_rescuesInFlightHungRetryToDlq() throws Exception {
        // A retry that has STARTED but is hung in a slow/unresponsive provider.uploadFile(...) at
        // shutdown must still have its intent persisted to the DLQ (not just scheduled-but-unrun
        // retries). Otherwise remote durability silently falls behind local state after a forced
        // shutdown under slow cloud I/O.
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean uploadStarted = new AtomicBoolean(false);
        CloudStorageProviderFactory.setActiveProviderForTest(new CapturingProvider() {
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
        });

        Path sstFile = tmpRoot.resolve("000077.sst");
        Files.createFile(sstFile);

        CloudUploadRetryQueue queue =
                new CloudUploadRetryQueue(3, 50L, 200L, tmpRoot.toString());
        // The scheduled retry starts quickly and then hangs inside uploadFile.
        queue.submit("db-h", "default", sstFile.toString(), "db-h/000077.sst",
                     new IOException("initial failure"));

        long deadline = System.currentTimeMillis() + 3_000L;
        while (!uploadStarted.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue("Retry upload must have started (in-flight)", uploadStarted.get());
        assertEquals("Nothing in the DLQ before shutdown", 0, queue.getDlqSize());

        try {
            // Force a fast shutdown; the in-flight (hung) retry must be rescued to the DLQ.
            queue.close(200, TimeUnit.MILLISECONDS);

            assertEquals("In-flight hung retry must be rescued to the DLQ on forced shutdown",
                         1, queue.getDlqSize());
            FailedUploadTask entry = queue.getDlqEntries().get(0);
            assertEquals("db-h", entry.getDbName());
            assertEquals("db-h/000077.sst", entry.getRemoteKey());
            assertTrue("DLQ entry should record the in-flight-at-shutdown reason: "
                       + entry.getLastError(),
                       entry.getLastError().contains("in flight"));
        } finally {
            release.countDown(); // let the hung thread exit
        }
    }

    // -----------------------------------------------------------------------
    // DLQ size cap – bounded under a sustained outage
    // -----------------------------------------------------------------------

    @Test
    public void dlq_boundedByCap_evictsOldestAndCountsDropped() throws Exception {
        // maxAttempts=0 routes every failure straight to the DLQ (the default provider-handles-
        // its-own-retries mode). Simulates a prolonged outage flooding the DLQ.
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            queue.setMaxDlqSize(5);

            for (int i = 0; i < 20; i++) {
                queue.submit("db", "cf", tmpRoot.resolve(i + ".sst").toString(),
                             "db/" + i + ".sst", new IOException("outage"));
            }

            assertEquals("DLQ must be capped at the configured max", 5, queue.getDlqSize());
            assertEquals("Every entry beyond the cap must be counted as dropped",
                         15, queue.getDroppedDlqCount());

            // The survivors must be the NEWEST entries (oldest evicted).
            for (FailedUploadTask t : queue.getDlqEntries()) {
                String key = t.getRemoteKey();
                int n = Integer.parseInt(key.substring("db/".length(), key.length() - ".sst".length()));
                assertTrue("Only the newest entries (>=15) must survive, saw: " + key, n >= 15);
            }

            // On-disk file must also be bounded (amortized rewrite compacts it).
            long lines;
            try (Stream<String> s = Files.lines(tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME))) {
                lines = s.filter(l -> !l.isBlank() && !l.startsWith("#")).count();
            }
            assertTrue("On-disk DLQ must be bounded (was " + lines + ")", lines <= 10);
        }
    }

    @Test
    public void dlqEnqueuedTotal_isMonotonic_andCountsEvictedEntries() {
        // The DLQ enqueue-rate backpressure signal relies on a monotonic total that only grows as
        // uploads EXHAUST their retries, unaffected by eviction (cap) draining the live DLQ. This is
        // what lets backpressure track active durability loss without pinning on a static,
        // post-recovery DLQ depth.
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            queue.setMaxDlqSize(5);
            assertEquals(0L, queue.getDlqEnqueuedTotal());

            for (int i = 0; i < 20; i++) {
                queue.submit("db", "cf", tmpRoot.resolve(i + ".sst").toString(),
                             "db/" + i + ".sst", new IOException("outage"));
            }

            // Live depth is capped at 5, but the cumulative enqueue total counts every exhausted
            // upload — including the 15 that were evicted.
            assertEquals("Live DLQ depth is capped", 5, queue.getDlqSize());
            assertEquals("Cumulative enqueue total must count all 20 exhausted uploads (incl. evicted)",
                         20L, queue.getDlqEnqueuedTotal());
        }
    }

    @Test
    public void dlq_rewriteIsAtomic_leavesNoTempAndValidFile() throws Exception {
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            queue.setMaxDlqSize(3);
            // >maxDlqSize appends force at least one amortized rewrite (compaction).
            for (int i = 0; i < 12; i++) {
                queue.submit("db", "cf", tmpRoot.resolve(i + ".sst").toString(),
                             "db/" + i + ".sst", new IOException("outage"));
            }

            // An atomic write-to-temp + rename must never leave the temp file behind.
            Path tmp = tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME + ".tmp");
            assertFalse("Atomic rewrite must not leave a .tmp file", Files.exists(tmp));

            // The persisted file must be well-formed and bounded.
            long lines;
            try (Stream<String> s =
                         Files.lines(tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME))) {
                lines = s.filter(l -> !l.isBlank() && !l.startsWith("#")).count();
            }
            assertTrue("On-disk DLQ must be bounded after atomic rewrite (was " + lines + ")",
                       lines <= 3);
            assertTrue("Persistence must remain healthy on success",
                       queue.isDlqPersistenceHealthy());
            assertEquals(0, queue.getDlqPersistenceFailureCount());
        }
    }

    @Test
    public void dlq_persistenceFailure_marksDegraded() throws Exception {
        // Pre-create a DIRECTORY where the DLQ file should live so every on-disk persist fails.
        Path dlqPath = tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME);
        Files.createDirectory(dlqPath);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            assertTrue("Healthy before any persist attempt", queue.isDlqPersistenceHealthy());

            queue.submit("db", "cf", tmpRoot.resolve("x.sst").toString(),
                         "db/x.sst", new IOException("outage"));

            // The in-memory DLQ still holds the entry; only the disk persist failed.
            assertEquals("Entry retained in memory despite disk failure", 1, queue.getDlqSize());
            assertFalse("Persist failure must flip health to degraded",
                        queue.isDlqPersistenceHealthy());
            assertTrue("Failure must be counted", queue.getDlqPersistenceFailureCount() >= 1);
        }
    }

    @Test
    public void dlq_healthRecoversOnlyViaFullRewrite_notPlainAppend() throws Exception {
        // A DIRECTORY at the DLQ path makes the first append fail -> degraded, with the entry left
        // memory-only. Health must NOT recover on a later plain append; it recovers only once a full
        // rewrite proves ALL outstanding entries (including the earlier memory-only one) are durable.
        Path dlqPath = tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME);
        Files.createDirectory(dlqPath);

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            // Entry A: append fails (path is a directory) -> degraded, A is memory-only.
            queue.submit("db", "cf", tmpRoot.resolve("a.sst").toString(),
                         "db/a.sst", new IOException("outage"));
            assertFalse("First failed append must degrade health", queue.isDlqPersistenceHealthy());
            assertEquals(1, queue.getDlqSize());

            // Free the path so writes can succeed again (transient failure resolved).
            Files.delete(dlqPath);

            // Entry B: while degraded, the code must do a FULL rewrite (persisting A and B), which
            // succeeds now and is the ONLY legitimate path back to healthy.
            queue.submit("db", "cf", tmpRoot.resolve("b.sst").toString(),
                         "db/b.sst", new IOException("outage"));

            assertTrue("Health must recover after a successful full rewrite persists all entries",
                       queue.isDlqPersistenceHealthy());
            assertEquals(2, queue.getDlqSize());

            // Both entries (including the previously memory-only A) must now be on disk.
            long lines;
            try (Stream<String> s =
                         Files.lines(tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME))) {
                lines = s.filter(line -> !line.isBlank() && !line.startsWith("#")).count();
            }
            assertEquals("Full rewrite must persist every outstanding DLQ entry", 2, lines);
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

    @Test
    public void dlq_reloadTrimmedToConfiguredCap() throws Exception {
        // Simulate a large persisted DLQ from a long outage: flood the first queue (maxAttempts=0
        // routes straight to DLQ) so many entries land on disk under the default cap.
        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            for (int i = 0; i < 30; i++) {
                queue.submit("db", "cf", tmpRoot.resolve(i + ".sst").toString(),
                             "db/" + i + ".sst", new IOException("outage"));
            }
            assertEquals("All 30 entries persist under the default cap", 30, queue.getDlqSize());
        }

        // Reconstruct: the load bounds to the default cap (all 30 fit), then the operator applies a
        // SMALLER configured cap, which must trim the loaded set and rewrite the on-disk file so a
        // large persisted DLQ cannot linger unbounded despite a tighter configuration.
        try (CloudUploadRetryQueue queue2 =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            assertEquals("Reload must recover all persisted entries under the default cap",
                         30, queue2.getDlqSize());

            queue2.setMaxDlqSize(5);
            assertEquals("Configured cap must trim the reloaded DLQ", 5, queue2.getDlqSize());

            // Only the newest entries survive.
            for (FailedUploadTask t : queue2.getDlqEntries()) {
                String key = t.getRemoteKey();
                int n = Integer.parseInt(
                        key.substring("db/".length(), key.length() - ".sst".length()));
                assertTrue("Only the newest entries (>=25) must survive, saw: " + key, n >= 25);
            }

            // The on-disk file must be rewritten down to the configured bound.
            long lines;
            try (Stream<String> s =
                         Files.lines(tmpRoot.resolve(CloudUploadRetryQueue.DLQ_FILE_NAME))) {
                lines = s.filter(l -> !l.isBlank() && !l.startsWith("#")).count();
            }
            assertEquals("On-disk DLQ must be rewritten to the configured cap", 5, lines);
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

    @Test
    public void replayDlq_fallsBackToSourceSstWhenStagingPinGone() throws Exception {
        // The staging pin (filePath) was cleaned up, but the original source SST still exists and is
        // uploadable. Replay must salvage the upload from sourceSstPath instead of dropping it.
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        Path source = tmpRoot.resolve("000005.sst");
        Files.write(source, "sstdata".getBytes());
        // Staging pin path that does NOT exist (already cleaned up).
        String pin = tmpRoot.resolve("000005.sst.upload-123").toString();

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            // maxAttempts=0 => submitPinned routes straight to the DLQ with filePath=pin (missing),
            // sourceSstPath=source (present).
            queue.submitPinned("db5", "default", pin, source.toString(), "db5/000005.sst",
                               new IOException("outage"));
            assertEquals(1, queue.getDlqSize());

            queue.replayDlq();

            assertEquals("Replay must salvage via sourceSstPath when the staging pin is gone",
                         0, queue.getDlqSize());
            assertTrue("Upload must have happened from the surviving source SST",
                       provider.uploads.stream()
                                       .anyMatch(u -> source.toString().equals(u[0])
                                                      && "db5/000005.sst".equals(u[1])));
        }
    }

    @Test
    public void replayDlq_dropsWhenNeitherPinNorSourceExists() {
        CapturingProvider provider = new CapturingProvider();
        CloudStorageProviderFactory.setActiveProviderForTest(provider);

        String pin = tmpRoot.resolve("gone.sst.upload-1").toString();
        String source = tmpRoot.resolve("gone.sst").toString();

        try (CloudUploadRetryQueue queue =
                     new CloudUploadRetryQueue(0, 50L, 50L, tmpRoot.toString())) {
            queue.submitPinned("db6", "default", pin, source, "db6/gone.sst",
                               new IOException("outage"));
            assertEquals(1, queue.getDlqSize());

            queue.replayDlq();

            assertEquals("Replay must drop only when NEITHER path exists", 0, queue.getDlqSize());
            assertTrue("No upload should occur when both paths are gone", provider.uploads.isEmpty());
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
                    "db\twith-tab", "cf-name",
                    "/data/root/file.sst.upload-99999", // pinnedPath
                    "/data/root/file.sst",              // sourceSstPath
                    "data/root/file.sst",               // remoteKey
                    1234567890123L, 3,
                    "Error with\nnewline and\\backslash",
                    42L);                               // uploadEpoch

            String line = queue.serialize(original);
            assertFalse("Serialised line must not contain raw tab in field values",
                        hasUnescapedTab(line));

            FailedUploadTask restored = queue.deserialize(line);
            assertNotNull(restored);
            assertEquals(original.getDbName(), restored.getDbName());
            assertEquals(original.getCfName(), restored.getCfName());
            assertEquals(original.getFilePath(), restored.getFilePath());
            assertEquals(original.getSourceSstPath(), restored.getSourceSstPath());
            assertEquals(original.getRemoteKey(), restored.getRemoteKey());
            assertEquals(original.getFailedAt(), restored.getFailedAt());
            assertEquals(original.getAttemptCount(), restored.getAttemptCount());
            assertEquals(original.getLastError(), restored.getLastError());
            assertEquals(original.getUploadEpoch(), restored.getUploadEpoch());
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Returns true if the serialised line has the wrong number of tab-separated fields. */
    private boolean hasUnescapedTab(String serialised) {
        String[] parts = serialised.split("\t", -1);
        // Format is now 9 fields: failedAt, attemptCount, dbName, cfName, filePath,
        // remoteKey, lastError, sourceSstPath, uploadEpoch.
        return parts.length != 9;
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









