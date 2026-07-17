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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory.LiveSstFile;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory.MetadataSnapshot;
import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * In-process integration test suite for the cloud storage subsystem.
 *
 * <h3>Design</h3>
 * All tests run without any network I/O. A {@link FakeCloudStore} replaces the real S3 provider:
 * it is an in-memory, thread-safe key-value store that faithfully implements every method of
 * {@link CloudStorageProvider}, including {@link CloudStorageProvider#deletePrefix} partial-failure
 * injection, {@link CloudStorageProvider#downloadFile} atomic-write simulation, and upload fault
 * injection. Every test seeds the fake store, runs the production listener, and makes assertions
 * directly against the fake store's state.
 *
 * <h3>Coverage</h3>
 * Each test targets a specific behaviour fixed by the review comments:
 * <ol>
 *   <li>{@code sst_uploadAndDownload_roundTrip} — basic upload/download produces identical content</li>
 *   <li>{@code preHydration_writesFilesAtomically} — temp file is never exposed at the final path</li>
 *   <li>{@code preHydration_removesStaleHydTmpFiles} — stale .hyd-tmp cleaned on next hydration</li>
 *   <li>{@code preHydration_multiRoot_usesCorrectDataRoot} — files go to the matching data root</li>
 *   <li>{@code tombstone_writtenAsSiblingOfDataPrefix} — key is {@code <prefix>_DELETED}, not {@code <prefix>/_DELETED}</li>
 *   <li>{@code tombstone_preventsHydrationOfDeletedGeneration} — stale data is never downloaded</li>
 *   <li>{@code tombstone_survivesPartialPurge} — tombstone is NOT deleted when purge throws</li>
 *   <li>{@code tombstone_deletedAfterCleanPurge} — tombstone IS deleted when purge succeeds</li>
 *   <li>{@code deletePrefix_partialFailure_reportedAsError} — partial S3 errors surface as IOException</li>
 *   <li>{@code metadata_publishOrder_currentLastAfterSsts} — CURRENT uploaded only after SSTs</li>
 *   <li>{@code metadata_staleSnapshot_rejectedByGenerationCheck} — older generation never overwrites newer</li>
 *   <li>{@code metadata_concurrentPublish_onlyLatestGenerationWins} — concurrent syncs serialized per-DB</li>
 *   <li>{@code retryQueue_failedUpload_retriedAndConfirmed} — retry queue eventually marks file confirmed</li>
 *   <li>{@code retryQueue_exhausted_sentToDlq} — exhausted retries land in DLQ, not lost silently</li>
 *   <li>{@code readMiss_hydration_restoresMissingLiveFile} — missing live SST is restored on read miss</li>
 *   <li>{@code readMiss_guardWindow_suppressesRepeatAttempts} — guard window dedups rapid re-hydration</li>
 *   <li>{@code deleteGuard_blocksDeleteUntilLiveSetDurable} — delete is held until replacements are in cloud</li>
 *   <li>{@code backpressure_slowsCallerWhenBacklogExceedsWatermark} — upload threads block under load</li>
 * </ol>
 */
@SuppressWarnings({"BusyWait", "ResultOfMethodCallIgnored"})
public class CloudStorageIntegrationTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Path dataRoot;
    private FakeCloudStore store;

    @Before
    public void setUp() throws Exception {
        dataRoot = tmp.newFolder("storage").toPath();
        store = new FakeCloudStore();
        CloudStorageProviderFactory.setActiveProviderForTest(store);
    }

    @After
    public void tearDown() {
        CloudStorageProviderFactory.reset();
    }

    // =========================================================================
    // 1. Basic SST upload / download round-trip
    // =========================================================================

    @Test
    public void sst_uploadAndDownload_roundTrip() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "row1=v1;row2=v2");

        CloudStorageEventListener listener = listenerFor(dataRoot);
        listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));
        awaitUpload(store);

        // Remote key must be relative to dataRoot.
        String relKey = dataRoot.relativize(sst).toString();
        assertTrue("SST must be uploaded to cloud", store.fileExists(relKey));

        // Download back to a different path and verify content.
        Path recovered = dataRoot.resolve("recovered.sst");
        store.downloadFile(relKey, recovered.toString());
        assertEquals("Recovered content must match original",
                     Files.readString(sst),
                     Files.readString(recovered));
    }

    // =========================================================================
    // 2. Pre-hydration writes files atomically (no partial file at final path)
    // =========================================================================

    @Test
    public void preHydration_writesFilesAtomically() throws Exception {
        // Seed the fake cloud with a database.
        String sstKey = "hugegraph/db/000001.sst";
        String currentKey = "hugegraph/db/CURRENT";
        String manifestKey = "hugegraph/db/MANIFEST-000001";
        store.put(sstKey, bytes("sst-body"));
        store.put(currentKey, bytes("MANIFEST-000001"));
        store.put(manifestKey, bytes("manifest-body"));

        Path dbDir = mkdirs("hugegraph/db");

        // Actively prove atomicity rather than assuming it: this provider asserts, at the moment
        // each download happens, that (a) the destination handed to it is a TEMP path (never the
        // final SST/CURRENT/MANIFEST path), and (b) the final path is not yet visible. If a
        // regression made hydration download straight to the final path, one of these fails.
        List<String> violations = new CopyOnWriteArrayList<>();
        FakeCloudStore atomicityChecker = new FakeCloudStore() {
            @Override
            public void downloadFile(String remoteKey, String localPath) throws IOException {
                Path finalPath = dataRoot.resolve(remoteKey);
                String tempName = Paths.get(localPath).getFileName().toString();
                if (!tempName.contains(".hyd-tmp")) {
                    violations.add("download target is not a temp file: " + localPath);
                }
                if (Files.exists(finalPath)) {
                    violations.add("final path visible during download: " + finalPath);
                }
                super.downloadFile(remoteKey, localPath);
                // Final path must still be absent until the production code performs the move.
                if (Files.exists(finalPath)) {
                    violations.add("final path materialised by download (no atomic move): "
                                   + finalPath);
                }
            }
        };
        // Copy the seeded objects into the checker store.
        atomicityChecker.put(sstKey, bytes("sst-body"));
        atomicityChecker.put(currentKey, bytes("MANIFEST-000001"));
        atomicityChecker.put(manifestKey, bytes("manifest-body"));
        CloudStorageProviderFactory.setActiveProviderForTest(atomicityChecker);

        CloudStorageEventListener listener = listenerFor(dataRoot);
        listener.onDBOpening("hugegraph", dbDir.toString());

        assertTrue("Atomicity violations detected: " + violations, violations.isEmpty());

        // All files must be present at their final paths with the correct content.
        assertEquals("SST content must match after atomic hydration",
                     "sst-body", Files.readString(dataRoot.resolve(sstKey)));
        assertEquals("CURRENT content must match after atomic hydration",
                     "MANIFEST-000001", Files.readString(dataRoot.resolve(currentKey)));
        assertEquals("MANIFEST content must match after atomic hydration",
                     "manifest-body", Files.readString(dataRoot.resolve(manifestKey)));

        // No hydration temp files must remain (atomic-move clean-up).
        assertNoHydTmpFiles(dataRoot);
    }

    // =========================================================================
    // 3. Pre-hydration removes stale .hyd-tmp files left by a previous crash
    // =========================================================================

    @Test
    public void preHydration_removesStaleHydTmpFiles() throws Exception {
        String sstKey = "hugegraph/db/000001.sst";
        store.put(sstKey, bytes("sst-body"));

        Path dbDir = mkdirs("hugegraph/db");

        // Plant a stale temp file using the production naming pattern
        // (.hyd-tmp-<threadId>-<nanoTime>) to verify cleanup handles the real format.
        Path staleTemp = dbDir.resolve("000001.sst.hyd-tmp-1-9999999999999");
        Files.write(staleTemp, bytes("partial-crash-data"));
        assertTrue("Stale temp file must exist before hydration", Files.exists(staleTemp));

        CloudStorageEventListener listener = listenerFor(dataRoot);
        listener.onDBOpening("hugegraph", dbDir.toString());

        // The stale temp file must have been removed.
        assertFalse("Stale .hyd-tmp must be removed before fresh download",
                    Files.exists(staleTemp));

        // The correct content must be at the final path.
        assertTrue("SST must be hydrated correctly", Files.exists(dbDir.resolve("000001.sst")));
        assertEquals("sst-body",
                     Files.readString(dbDir.resolve("000001.sst")));
    }

    // =========================================================================
    // 4. Pre-hydration with multiple data roots uses the correct root
    // =========================================================================

    @Test
    public void preHydration_multiRoot_usesCorrectDataRoot() throws Exception {
        // Two data roots: primary and secondary.
        Path primaryRoot = tmp.newFolder("storage0").toPath();
        Path secondaryRoot = tmp.newFolder("storage1").toPath();

        // DB lives under the secondary root.
        Path dbDir = secondaryRoot.resolve("hugegraph/db");
        Files.createDirectories(dbDir);

        // The remote key was built by stripping secondaryRoot, so it is "hugegraph/db/000001.sst".
        String sstKey = "hugegraph/db/000001.sst";
        store.put(sstKey, bytes("secondary-sst-body"));
        store.put("hugegraph/db/CURRENT", bytes("MANIFEST-000001"));
        store.put("hugegraph/db/MANIFEST-000001", bytes("manifest"));

        CloudStorageEventListener listener = listenerFor(
                Arrays.asList(primaryRoot.toString(), secondaryRoot.toString()));

        listener.onDBOpening("hugegraph", dbDir.toString());

        // File must land under secondaryRoot, not primaryRoot.
        assertTrue("SST must be hydrated under the matching secondary data root",
                   Files.exists(secondaryRoot.resolve(sstKey)));
        assertFalse("SST must NOT be placed under the primary data root",
                    Files.exists(primaryRoot.resolve(sstKey)));
    }

    // =========================================================================
    // 5. Tombstone key format: sibling (prefix_DELETED), not child (prefix/_DELETED)
    // =========================================================================

    @Test
    public void tombstone_writtenAsSiblingOfDataPrefix() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        CloudStorageEventListener listener = listenerFor(dataRoot);

        listener.onDBDeleteBegin("hugegraph", dbDir.toString());

        // Sibling key must be uploaded.
        String siblingKey = "hugegraph/db" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        assertTrue("Tombstone must be written as sibling key '" + siblingKey + "'",
                   store.fileExists(siblingKey));

        // Inner key must NOT be uploaded.
        String innerKey = "hugegraph/db/" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        assertFalse("Tombstone must NOT be written inside the data prefix '" + innerKey + "'",
                    store.fileExists(innerKey));
    }

    // =========================================================================
    // 6. Tombstone prevents hydration of a deleted DB generation
    // =========================================================================

    @Test
    public void tombstone_preventsHydrationOfDeletedGeneration() throws Exception {
        // Simulate a deleted DB: stale SSTs and a tombstone in cloud.
        store.put("hugegraph/db/000001.sst", bytes("stale-data"));
        store.put("hugegraph/db/CURRENT", bytes("MANIFEST-000001"));
        store.put("hugegraph/db" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX,
                  bytes("deleted"));

        Path dbDir = mkdirs("hugegraph/db");
        CloudStorageEventListener listener = listenerFor(dataRoot);
        listener.onDBOpening("hugegraph", dbDir.toString());

        // Stale SST must NOT be downloaded.
        assertFalse("Stale SST must not be hydrated when tombstone is present",
                    Files.exists(dbDir.resolve("000001.sst")));
        assertFalse("Stale CURRENT must not be hydrated when tombstone is present",
                    Files.exists(dbDir.resolve("CURRENT")));

        // Stale data prefix must be purged.
        assertFalse("Stale SST must be purged from cloud after tombstone detection",
                    store.fileExists("hugegraph/db/000001.sst"));
    }

    // =========================================================================
    // 7. Tombstone is NOT deleted when the prefix purge fails (partial purge guard)
    // =========================================================================

    @Test
    public void tombstone_survivesPartialPurge() throws Exception {
        // Seed cloud with data prefix objects.
        store.put("hugegraph/db/000001.sst", bytes("sst"));
        store.put("hugegraph/db/CURRENT", bytes("MANIFEST-000001"));
        // Plant tombstone at sibling path.
        String tombstoneKey = "hugegraph/db" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        store.put(tombstoneKey, bytes("deleted"));

        // Make deletePrefix throw to simulate a partial purge.
        store.setPrefixDeleteFailure(true);

        Path dbDir = mkdirs("hugegraph/db");
        CloudStorageEventListener listener = listenerFor(dataRoot);
        listener.onDBDeleted("hugegraph", dbDir.toString());

        // Purge failed → tombstone must still be present in cloud.
        assertTrue("Tombstone must survive a failed prefix purge",
                   store.fileExists(tombstoneKey));
        // Stale SST must still be present (purge failed).
        assertTrue("Stale SST must remain when purge fails",
                   store.fileExists("hugegraph/db/000001.sst"));

        // Now a re-open must be blocked by the surviving tombstone: the listener finds the
        // tombstone, tries to purge (still fails), and throws IllegalStateException to fail
        // DB open rather than silently proceeding on empty local state.
        Files.createDirectories(dbDir);
        CloudStorageEventListener listener2 = listenerFor(dataRoot);
        try {
            listener2.onDBOpening("hugegraph", dbDir.toString());
            fail("Expected IllegalStateException: purge failure must block DB open");
        } catch (IllegalStateException e) {
            assertTrue("Exception message must describe purge failure",
                       e.getMessage().contains("purge failed"));
        }
        assertFalse("Stale SST must NOT be hydrated when tombstone-guarded purge fails",
                    Files.exists(dbDir.resolve("000001.sst")));
    }

    // =========================================================================
    // 8. Tombstone IS deleted after a successful purge
    // =========================================================================

    @Test
    public void tombstone_deletedAfterCleanPurge() throws Exception {
        store.put("hugegraph/db/000001.sst", bytes("sst"));
        String tombstoneKey = "hugegraph/db" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        store.put(tombstoneKey, bytes("deleted"));

        Path dbDir = mkdirs("hugegraph/db");
        CloudStorageEventListener listener = listenerFor(dataRoot);
        listener.onDBDeleted("hugegraph", dbDir.toString());

        // Purge succeeded → tombstone must be cleaned up.
        assertFalse("Tombstone must be removed after successful purge",
                    store.fileExists(tombstoneKey));
        assertFalse("Data SST must be purged", store.fileExists("hugegraph/db/000001.sst"));
    }

    // =========================================================================
    // 9. deletePrefix partial failure: per-key errors surface as IOException
    // =========================================================================

    @Test
    public void deletePrefix_partialFailure_reportedAsError() {
        // Seed 3 objects; make the store report errors for 1 of them.
        store.put("hugegraph/db/000001.sst", bytes("sst1"));
        store.put("hugegraph/db/000002.sst", bytes("sst2"));
        store.put("hugegraph/db/000003.sst", bytes("sst3"));
        store.setPartialDeleteErrors(Collections.singletonList("hugegraph/db/000002.sst"));

        try {
            store.deletePrefix("hugegraph/db/");
            fail("Expected IOException for partial delete failure");
        } catch (IOException e) {
            assertTrue("Error must name the failed key",
                       e.getMessage().contains("000002.sst"));
        }

        // The two successfully deleted keys must be gone; the failed one must survive.
        assertFalse("000001.sst must be deleted", store.fileExists("hugegraph/db/000001.sst"));
        assertFalse("000003.sst must be deleted", store.fileExists("hugegraph/db/000003.sst"));
        assertTrue("000002.sst must survive a per-key delete failure",
                   store.fileExists("hugegraph/db/000002.sst"));
    }

    // =========================================================================
    // 10. Metadata publish order: SSTs before MANIFEST, MANIFEST before CURRENT
    // =========================================================================

    @Test
    public void metadata_publishOrder_currentLastAfterSsts() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path tempDir = mkdirs("hugegraph/db-checkpoint");

        // Create SST and metadata stubs in the checkpoint directory.
        writeSst(tempDir, "000001.sst", "data");
        Files.write(tempDir.resolve("OPTIONS-000002"), "options".getBytes(StandardCharsets.UTF_8));
        Files.write(tempDir.resolve("MANIFEST-000001"),
                    "manifest".getBytes(StandardCharsets.UTF_8));
        Files.write(tempDir.resolve("CURRENT"), "MANIFEST-000001".getBytes(StandardCharsets.UTF_8));

        MetadataSnapshot snapshot = new MetadataSnapshot(
                dbDir.toString(),
                tempDir.toString(),
                "CURRENT",
                "MANIFEST-000001",
                Collections.singletonList("OPTIONS-000002"),
                Collections.singletonList("000001.sst"),
                1L);

        // Use a recording store to capture upload order.
        OrderedRecordingStore ordered = new OrderedRecordingStore(store);
        CloudStorageProviderFactory.setActiveProviderForTest(ordered);

        CloudStorageEventListener listener = listenerFor(dataRoot);
        boolean published = listener.uploadMetadataSnapshot(ordered, "hugegraph", snapshot);

        assertTrue("Metadata publish must succeed", published);

        List<String> uploadOrder = ordered.uploadOrder();
        // CURRENT must be the last metadata file uploaded.
        int currentIdx  = lastIndexOf(uploadOrder, "CURRENT");
        int manifestIdx = lastIndexOf(uploadOrder, "MANIFEST-000001");
        int sstIdx      = lastIndexOf(uploadOrder, "000001.sst");

        assertTrue("SSTs must be uploaded before MANIFEST", sstIdx < manifestIdx);
        assertTrue("MANIFEST must be uploaded before CURRENT", manifestIdx < currentIdx);
    }

    // =========================================================================
    // 11. Stale snapshot rejected by generation check
    // =========================================================================

    @Test
    public void metadata_staleSnapshot_rejectedByGenerationCheck() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path tempDir = mkdirs("hugegraph/db-checkpoint");

        CloudStorageEventListener listener = listenerFor(dataRoot);

        // Publish generation 10 to establish the watermark.
        MetadataSnapshot gen10 = snapshot(dbDir, tempDir, "MANIFEST-000010", 10L);
        assertTrue("Generation 10 must publish successfully",
                   listener.uploadMetadataSnapshot(store, "hugegraph", gen10));

        // Attempt to publish generation 5 (older than the watermark).
        // uploadMetadataSnapshot itself does not carry the generation guard — that lives in
        // syncMetadataSnapshotInline. Verify the guard is effective by calling it directly:
        // the generation guard is in publishSnapshotWithGenerationCheck which is called from
        // syncMetadataSnapshotInline. We exercise it by checking that the listener rejects
        // a snapshot whose generation is below lastPublishedMetadataGeneration.
        //
        // We call uploadMetadataSnapshot with gen5 directly to confirm it succeeds on its own
        // (no guard there), then verify syncMetadataSnapshotInline would block re-publishing
        // by checking no extra cloud writes occur when the snapshot generation is old.
        MetadataSnapshot gen5 = snapshot(dbDir, tempDir, "MANIFEST-000005", 5L);
        // Direct upload of gen5 succeeds (uploadMetadataSnapshot has no generation check).
        listener.uploadMetadataSnapshot(store, "hugegraph", gen5);

        // Now re-publish gen10 — the high-water mark must stay at 10 (not regress to 5).
        // Verify by publishing gen10 again; it must succeed and not corrupt the watermark.
        assertTrue("Re-publishing generation 10 must succeed after gen5 direct upload",
                   listener.uploadMetadataSnapshot(store, "hugegraph", gen10));

        // Build a second listener that has the watermark already at 10 and attempt
        // to push gen5 through syncMetadataSnapshotInline using an override.
        // We use a subclass seam: override captureMetadataSnapshot to supply gen5.
        CloudStorageEventListener guardedListener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), 0) {
            private boolean gen10Published = false;

            @Override
            MetadataSnapshot captureMetadataSnapshot(String dbName) {
                // First call: return gen10 to establish the watermark.
                // Second call: return gen5 to trigger the rejection.
                if (!gen10Published) {
                    gen10Published = true;
                    return gen10;
                }
                return gen5;
            }
        };

        // First sync establishes the watermark at generation 10.
        guardedListener.syncMetadataSnapshotInline(store, "hugegraph");
        int uploadsAtWatermark = store.totalUploads();

        // Second sync presents generation 5 — the guard must reject it without uploading.
        guardedListener.syncMetadataSnapshotInline(store, "hugegraph");
        assertEquals("No uploads must occur when a stale generation is rejected",
                     uploadsAtWatermark, store.totalUploads());
    }

    // =========================================================================
    // 12. Concurrent metadata publishes for the same DB are serialized
    // =========================================================================

    @Test
    public void metadata_concurrentPublish_onlyLatestGenerationWins() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");

        // Drive the concurrency through the REAL guarded path — syncMetadataSnapshotInline, which
        // owns the per-DB lock and the generation guard — rather than uploadMetadataSnapshot()
        // directly (which has neither). Each thread owns a fixed generation with its own checkpoint
        // dir + distinct MANIFEST, handed to the listener via captureMetadataSnapshot so the guard
        // sees a genuine capture->publish flow. This way a regression in the serialization or
        // generation logic makes the test fail instead of silently passing.
        final int threads = 6;
        Map<Long, MetadataSnapshot> snapshotsByGen = new ConcurrentHashMap<>();
        for (long gen = 1; gen <= threads; gen++) {
            Path tempDir = mkdirs("hugegraph/db-checkpoint-" + gen);
            snapshotsByGen.put(gen,
                               snapshot(dbDir, tempDir, String.format("MANIFEST-%06d", gen), gen));
        }

        // Each worker thread publishes exactly one fixed generation; captureMetadataSnapshot returns
        // the calling thread's snapshot so syncMetadataSnapshotInline's guard is exercised.
        ThreadLocal<MetadataSnapshot> perThread = new ThreadLocal<>();
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), 0) {
            @Override
            MetadataSnapshot captureMetadataSnapshot(String dbName) {
                return perThread.get();
            }
        };

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go    = new CountDownLatch(1);
        List<Exception> errors = new CopyOnWriteArrayList<>();
        List<Boolean> publishResults = new CopyOnWriteArrayList<>();

        for (long g = 1; g <= threads; g++) {
            final long gen = g;
            pool.submit(() -> {
                try {
                    perThread.set(snapshotsByGen.get(gen));
                    ready.countDown();
                    go.await();
                    publishResults.add(listener.syncMetadataSnapshotInline(store, "hugegraph"));
                } catch (Exception e) {
                    errors.add(e);
                }
            });
        }

        assertTrue("Workers must be ready in 5s", ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue("All threads must finish in 10s", pool.awaitTermination(10, TimeUnit.SECONDS));

        assertTrue("No concurrent publish errors: " + errors, errors.isEmpty());

        // The highest generation can never be stale, so it always publishes; once it does, every
        // lower generation captured afterwards is rejected by the guard. Whatever the
        // (nondeterministic) lock-acquisition order, the durable CURRENT must therefore point at the
        // MAX generation's MANIFEST — a stale generation can never be the last writer.
        String winningManifest = String.format("MANIFEST-%06d", (long) threads);
        assertTrue("CURRENT must be present in cloud after concurrent publishes",
                   store.fileExists("hugegraph/db/CURRENT"));
        assertEquals("Cloud CURRENT must point at the highest generation's manifest — only the "
                     + "non-stale generation may win",
                     winningManifest, store.readCurrentUtf8());
        assertTrue("The winning manifest object must be durable in cloud",
                   store.fileExists("hugegraph/db/" + winningManifest));

        long published = publishResults.stream().filter(Boolean::booleanValue).count();
        assertTrue("At least the max generation must have published", published >= 1);
        assertTrue("No more successful publishes than offered generations", published <= threads);
    }

    // =========================================================================
    // 13. Retry queue retries a failed upload and eventually marks it confirmed
    // =========================================================================

    @Test
    public void retryQueue_failedUpload_retriedAndConfirmed() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "data");

        // First call fails; subsequent calls succeed.
        AtomicInteger calls = new AtomicInteger(0);
        FakeCloudStore flakyStore = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                if (calls.incrementAndGet() == 1) {
                    throw new IOException("transient failure");
                }
                super.uploadFile(localPath, remoteKey);
            }
        };
        CloudStorageProviderFactory.setActiveProviderForTest(flakyStore);

        CloudSyncTracker tracker = new CloudSyncTracker();
        // Wire the SAME epoch-aware callback production uses (AppConfig wires
        // syncTracker::markConfirmedIfEpoch). This ensures the test fails if the retry path drops
        // the epoch — the plain 2-arg callback would confirm regardless and hide that defect.
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                3, 10L, 100L,
                dataRoot.toString(),
                tracker::markConfirmedIfEpoch);

        // Capture the epoch at submission time and pass it through, exactly as the production
        // callers in CloudStorageEventListener do.
        long uploadEpoch = tracker.currentEpoch("hugegraph");
        retryQueue.submit("hugegraph", "default", sst.toString(),
                          "hugegraph/db/000001.sst", uploadEpoch,
                          new IOException("initial failure"));

        // Wait for the retry queue to process.
        boolean confirmed = awaitCondition(() -> tracker.isConfirmed("hugegraph", sst.toString()),
                                           3_000);
        assertTrue("SST must be confirmed after retry", confirmed);
        assertTrue("SST must be in cloud after retry",
                   flakyStore.fileExists("hugegraph/db/000001.sst"));

        retryQueue.close();
    }

    @Test
    public void tableCreate_providerUnavailableInitially_recoversWhenProviderReturns()
            throws Exception {
        // If the provider is briefly unavailable when a new SST appears, the upload must be
        // staged through the retry queue (not silently dropped) and complete once provider
        // availability recovers.
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000011.sst", "retry-after-provider-down");

        CloudSyncTracker tracker = new CloudSyncTracker();
        try (CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                4, 100L, 400L, dataRoot.toString(), tracker::markConfirmedIfEpoch)) {

            CloudStorageEventListener listener = new CloudStorageEventListener(
                    Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker, 0);

            CloudStorageProviderFactory.setActiveProviderForTest(null);
            listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));

            // Restore provider after the first retry(s) have seen the outage.
            Thread restorer = new Thread(() -> {
                try {
                    Thread.sleep(350L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                CloudStorageProviderFactory.setActiveProviderForTest(store);
            });
            restorer.setDaemon(true);
            restorer.start();

            boolean uploaded = awaitCondition(
                    () -> store.fileExists("hugegraph/db/000011.sst"), 8_000);
            restorer.join(2_000);

            assertTrue("SST upload must recover automatically once provider returns", uploaded);
            assertEquals("Recovered upload must not be stranded in DLQ", 0, retryQueue.getDlqSize());
        }
    }

    @Test
    public void asyncUploadFailure_retryUsesStagedPin_evenAfterOriginalSstDeleted() throws Exception {
        // Regression guard for the compaction-race durability hole: when the async upload fails, the
        // retry must upload from the staged hard-link PIN, not the original SST path. If it used the
        // original (which compaction can delete before the retry fires) the retry would be silently
        // dropped and the byteset would never reach cloud.
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "data-bytes");

        // First async upload attempt fails; the retry (from the pin) succeeds.
        AtomicInteger calls = new AtomicInteger(0);
        FakeCloudStore flakyStore = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                if (calls.incrementAndGet() == 1) {
                    throw new IOException("transient first-attempt failure");
                }
                super.uploadFile(localPath, remoteKey);
            }
        };
        CloudStorageProviderFactory.setActiveProviderForTest(flakyStore);

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                3, 10L, 100L, dataRoot.toString(),
                tracker::markConfirmedIfEpoch);
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker, 0);

        // Dispatch the async upload: onTableFileCreated creates the staged pin, then the first
        // attempt fails and (with the fix) hands the PIN to the retry queue.
        listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));

        // Simulate compaction deleting the original SST before the retry fires. The pin (a hard link)
        // keeps the byteset alive, so the retry must still succeed.
        Files.deleteIfExists(sst);
        assertFalse("Original SST must be gone (compacted) before the retry", Files.exists(sst));

        boolean confirmed = awaitCondition(
                () -> tracker.isConfirmed("hugegraph", sst.toString()), 5_000);
        assertTrue("Retry must upload from the staged pin and confirm even after the original SST "
                   + "is deleted", confirmed);
        assertTrue("SST must be present in cloud after the pin-based retry",
                   flakyStore.fileExists("hugegraph/db/000001.sst"));

        retryQueue.close();
    }

    @Test
    public void retrySuccess_publishesUpdatedMetadata() throws Exception {
        // A retry that uploads the SST during a quiet period must also advance the mirrored
        // recovery point: CURRENT/MANIFEST must be published, not just the tracker confirmed.
        Path dbDir = mkdirs("hugegraph/db");
        Path tempDir = mkdirs("hugegraph/db-checkpoint");
        Path sst = writeSst(dbDir, "000001.sst", "data");

        // First SST upload attempt (via retry) fails; the retry then succeeds.
        AtomicInteger calls = new AtomicInteger(0);
        FakeCloudStore flakyStore = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                if (calls.incrementAndGet() == 1) {
                    throw new IOException("transient first-attempt failure");
                }
                super.uploadFile(localPath, remoteKey);
            }
        };
        CloudStorageProviderFactory.setActiveProviderForTest(flakyStore);

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = createRetryQueue(tracker, dbDir, tempDir);

        long epoch = tracker.currentEpoch("hugegraph");
        retryQueue.submit("hugegraph", "default", sst.toString(), "hugegraph/db/000001.sst",
                          epoch, new IOException("initial failure"));

        boolean confirmed = awaitCondition(
                () -> tracker.isConfirmed("hugegraph", sst.toString()), 5_000);
        assertTrue("SST must be confirmed after retry", confirmed);

        // The retry success must ALSO trigger a metadata publish so cloud CURRENT catches up.
        boolean published = awaitCondition(
                () -> flakyStore.fileExists("hugegraph/db/CURRENT"), 5_000);
        assertTrue("Retry success must publish CURRENT/MANIFEST (recovery point must advance, not "
                   + "just the tracker confirmation)", published);
        assertEquals("Published CURRENT must reference the captured generation's manifest",
                     "MANIFEST-000005", flakyStore.readCurrentUtf8());

        retryQueue.close();
    }

    private @NotNull CloudUploadRetryQueue createRetryQueue(CloudSyncTracker tracker, Path dbDir,
                                                            Path tempDir) {
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                3, 10L, 100L, dataRoot.toString(),
                tracker::markConfirmedIfEpoch);

        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker, 0) {
            @Override
            MetadataSnapshot captureMetadataSnapshot(String db) {
                try {
                    return snapshot(dbDir, tempDir, "MANIFEST-000005", 5L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        // Wire the retry-success -> metadata-sync trigger exactly as AppConfig does.
        retryQueue.setMetadataSyncTrigger(listener::onRetryUploadDurable);
        return retryQueue;
    }

    // =========================================================================
    // 14. Retry queue sends to DLQ when all retries exhausted
    // =========================================================================

    @Test
    public void retryQueue_exhausted_sentToDlq() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "data");

        // Always fail.
        FakeCloudStore alwaysFail = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                throw new IOException("permanent failure");
            }
        };
        CloudStorageProviderFactory.setActiveProviderForTest(alwaysFail);

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                2, 10L, 50L,
                dataRoot.toString(),
                tracker::markConfirmed);

        retryQueue.submit("hugegraph", "default", sst.toString(),
                          "hugegraph/db/000001.sst", new IOException("initial failure"));

        // Wait until the DLQ grows.
        boolean inDlq = awaitCondition(() -> retryQueue.getDlqSize() > 0, 5_000);
        assertTrue("Exhausted upload must be in DLQ", inDlq);
        assertFalse("File must not be confirmed when in DLQ",
                    tracker.isConfirmed("hugegraph", sst.toString()));

        retryQueue.close();
    }

    // =========================================================================
    // 15. Read-miss hydration restores a missing live SST file
    // =========================================================================

    @Test
    public void readMiss_hydration_restoresMissingLiveFile() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = dbDir.resolve("000001.sst");  // does NOT exist on disk yet

        // Cloud has the SST.
        String remoteKey = "hugegraph/db/000001.sst";
        store.put(remoteKey, bytes("row1=v1;row2=v2"));

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, tracker, 0);

        List<LiveSstFile> liveFiles = Collections.singletonList(
                new LiveSstFile(sst.toString(), "default"));

        int restored = listener.restoreMissingLiveFiles(store, "hugegraph", liveFiles);

        assertEquals("One live SST must be restored", 1, restored);
        assertTrue("Restored SST must exist at original path", Files.exists(sst));
        assertEquals("row1=v1;row2=v2",
                     Files.readString(sst));
        assertTrue("Restored SST must be marked confirmed in tracker",
                   tracker.isConfirmed("hugegraph", sst.toString()));
    }

    // =========================================================================
    // 16. Read-miss guard window suppresses rapid repeated hydration attempts
    // =========================================================================

    @Test
    public void readMiss_guardWindow_suppressesRepeatAttempts() throws Exception {
        long guardWindowMs = 1_000L;
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, guardWindowMs, null, new CloudSyncTracker(), 0);

        // First attempt: allowed.
        assertTrue("First read-miss attempt must be allowed",
                   listener.shouldAttemptReadMissHydration("hugegraph", "default"));

        // Second attempt within the guard window: suppressed.
        assertFalse("Second read-miss within guard window must be suppressed",
                    listener.shouldAttemptReadMissHydration("hugegraph", "default"));

        // After the guard window expires: allowed again.
        Thread.sleep(guardWindowMs + 50);
        assertTrue("Read-miss attempt after guard window expiry must be allowed",
                   listener.shouldAttemptReadMissHydration("hugegraph", "default"));
    }

    // =========================================================================
    // 17. Delete guard holds delete until all live-set files are durable
    // =========================================================================

    @Test
    public void deleteGuard_blocksDeleteUntilLiveSetDurable() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst1 = writeSst(dbDir, "000001.sst", "old-data");
        Path sst2 = writeSst(dbDir, "000002.sst", "new-data");

        // sst1 is confirmed in cloud; sst2 is not yet.
        CloudSyncTracker tracker = new CloudSyncTracker();
        tracker.markConfirmed("hugegraph", sst1.toString());

        store.put("hugegraph/db/000001.sst", bytes("old-data"));

        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, tracker, 0);

        List<LiveSstFile> liveFiles = Arrays.asList(
                new LiveSstFile(sst1.toString(), "default"),
                new LiveSstFile(sst2.toString(), "default"));

        // Live set not fully durable (sst2 unconfirmed and present locally).
        // ensureLiveSetUploaded must upload sst2 and return true.
        boolean durable = listener.ensureLiveSetUploaded(store, "hugegraph", liveFiles);
        assertTrue("Live set must be durable after ensureLiveSetUploaded", durable);
        assertTrue("sst2 must be uploaded to cloud",
                   store.fileExists("hugegraph/db/000002.sst"));
        assertTrue("sst2 must be marked confirmed after upload",
                   tracker.isConfirmed("hugegraph", sst2.toString()));
    }

    // =========================================================================
    // 17b. A leaked "truncating" latch self-heals so cloud deletes/mirroring resume
    // =========================================================================

    @Test
    public void truncatingLatch_selfHealsWhenTruncationNeverCompletes() throws Exception {
        // Simulate RocksDBSession.truncate() throwing between onDBTruncateBegin and onDBTruncated
        // (dropTables/createTables raise the unchecked DBStoreException with no finally), which
        // leaves the "truncating" latch stuck. Unlike the 5s grace period, the latch is UNBOUNDED,
        // so a stuck latch would silently disable cloud object deletion + metadata mirroring for
        // this DB for the lifetime of the process. The fix bounds the latch with a max window.
        Path dbDir = mkdirs("hugegraph/db");

        CloudStorageEventListener listener = listenerFor(dataRoot);
        // Shrink the stale-latch window so the test does not wait a real minute.
        listener.setMaxTruncationDurationMsForTest();

        // Begin truncation but never complete it — the latch is now set and, without the fix,
        // would stay set forever.
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        assertTrue("Latch must be active immediately after truncate begins",
                   listener.isActivelyTruncating("hugegraph"));

        // Wait past the stale-latch window: the leaked latch must self-heal so cloud metadata
        // mirroring, object deletion, and the delete guard resume for this DB.
        Thread.sleep(80L);
        assertFalse("Stale truncating latch must self-heal after the max window (else cloud "
                    + "mirroring/deletes are disabled for this DB forever)",
                    listener.isActivelyTruncating("hugegraph"));

        // A brand-new truncation still latches correctly (self-heal must not be sticky).
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        assertTrue("A fresh truncate must re-arm the latch",
                   listener.isActivelyTruncating("hugegraph"));
    }

    @Test
    public void truncateAbort_clearsLatchImmediately_withoutPurgingRemote() throws Exception {
        // Source-level guarantee: RocksDBSession.truncate() now calls notifyTruncateAbort in a
        // finally when drop/create throws, so the listener clears suppression at once — no need to
        // wait for the stale-latch window — and must NOT purge remote (data may still exist locally).
        Path dbDir = mkdirs("hugegraph/db");
        // Seed remote data that a wrongful purge-on-abort would destroy.
        store.put("hugegraph/db/000001.sst", bytes("still-needed"));

        CloudStorageEventListener listener = listenerFor(dataRoot);

        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        assertTrue("Latch must be active after truncate begins",
                   listener.isActivelyTruncating("hugegraph"));

        // Simulate truncate() failing partway → finally fires the abort notification.
        listener.onDBTruncateAbort("hugegraph", dbDir.toString());

        assertFalse("Abort must clear the truncating latch immediately",
                    listener.isActivelyTruncating("hugegraph"));
        assertFalse("Abort must not leave the DB in the truncation grace period",
                    listener.isInTruncationGracePeriodForTest("hugegraph"));
        assertTrue("Abort must NOT purge remote state (data may still be present locally)",
                   store.fileExists("hugegraph/db/000001.sst"));
    }

    @Test
    public void truncatePurgeFailure_retriesUntilRemoteIsPurged() throws Exception {
        // graph.clear() → onDBTruncated must purge the remote prefix. If that purge fails, stale
        // remote objects could resurrect cleared data on a future restore, so the failure must not
        // be ignored: the listener retries the purge until it succeeds.
        Path dbDir = mkdirs("hugegraph/db");
        store.put("hugegraph/db/000001.sst", bytes("cleared-data"));
        store.put("hugegraph/db/CURRENT", bytes("MANIFEST-000001"));

        CloudStorageEventListener listener = listenerFor(dataRoot);

        // First purge attempt fails (simulated transient cloud error).
        store.setPrefixDeleteFailure(true);
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        listener.onDBTruncated("hugegraph", dbDir.toString());

        assertTrue("Stale object must still be present right after a failed purge",
                   store.fileExists("hugegraph/db/000001.sst"));

        // Recovery: subsequent purges succeed; the scheduled retry must clean up the stale objects.
        store.setPrefixDeleteFailure(false);
        boolean purged = awaitCondition(
                () -> !store.fileExists("hugegraph/db/000001.sst"), 8_000);
        assertTrue("A failed truncate purge must be retried until the stale remote prefix is clean",
                   purged);
    }

    @Test
    public void truncatePurgeRetry_survivesTransientNullProvider() throws Exception {
        // A transient "provider unavailable" window (getActiveProvider() == null) during the retry
        // must NOT abandon the purge — the retry chain must reschedule and eventually complete once
        // the provider is back. Otherwise stale pre-truncate objects would remain permanently and
        // could resurrect cleared data after the suppression windows expire.
        Path dbDir = mkdirs("hugegraph/db");
        store.put("hugegraph/db/000001.sst", bytes("cleared-data"));

        CloudStorageEventListener listener = listenerFor(dataRoot);

        // First purge attempt fails → schedules a retry.
        store.setPrefixDeleteFailure(true);
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        listener.onDBTruncated("hugegraph", dbDir.toString());
        assertTrue("Stale object must remain after the failed purge",
                   store.fileExists("hugegraph/db/000001.sst"));

        // Simulate the provider being unavailable during the early retry attempts.
        CloudStorageProviderFactory.setActiveProviderForTest(null);

        // Restore a healthy provider shortly after, from a background thread.
        Thread restorer = new Thread(() -> {
            try {
                Thread.sleep(900L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            store.setPrefixDeleteFailure(false);
            CloudStorageProviderFactory.setActiveProviderForTest(store);
        });
        restorer.setDaemon(true);
        restorer.start();

        boolean purged = awaitCondition(
                () -> !store.fileExists("hugegraph/db/000001.sst"), 12_000);
        restorer.join(2_000);
        assertTrue("Retry chain must survive a transient null-provider window and eventually purge",
                   purged);
    }

    @Test
    public void truncatePurgeRetry_providerOutageLongerThanRetryBudget_stillPurges()
            throws Exception {
        // Regression guard: provider-unavailable windows must not consume truncate purge attempts.
        // Old behavior consumed retries on null provider and could stop after ~15.5s
        // (0.5 + 1 + 2 + 4 + 8) before provider recovery.
        Path dbDir = mkdirs("longoutage/db");
        store.put("longoutage/db/000001.sst", bytes("cleared-data"));

        CloudStorageEventListener listener = listenerFor(dataRoot);

        // First purge attempt fails and schedules retry chain.
        store.setPrefixDeleteFailure(true);
        listener.onDBTruncateBegin("longoutage", dbDir.toString());
        listener.onDBTruncated("longoutage", dbDir.toString());
        assertTrue("Stale object must remain after initial failed purge",
                   store.fileExists("longoutage/db/000001.sst"));

        // Allow purges to succeed once provider comes back, but keep provider unavailable longer
        // than the old bounded retry budget window.
        store.setPrefixDeleteFailure(false);
        CloudStorageProviderFactory.setActiveProviderForTest(null);

        Thread restorer = new Thread(() -> {
            try {
                Thread.sleep(18_000L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            CloudStorageProviderFactory.setActiveProviderForTest(store);
        });
        restorer.setDaemon(true);
        restorer.start();

        boolean purged = awaitCondition(
                () -> !store.fileExists("longoutage/db/000001.sst"), 26_000L);
        restorer.join(2_000L);
        assertTrue("Truncate purge must still complete after an outage longer than retry budget",
                   purged);
    }

    @Test
    public void truncatePurgeRetry_whenProviderMissingInitially_stillPurgesLater() throws Exception {
        // If provider is unavailable on the initial onDBTruncated callback, purge must still be
        // scheduled and completed once provider comes back.
        Path dbDir = mkdirs("nullfirst/db");
        store.put("nullfirst/db/000001.sst", bytes("stale-data"));

        CloudStorageEventListener listener = listenerFor(dataRoot);

        CloudStorageProviderFactory.setActiveProviderForTest(null);
        listener.onDBTruncateBegin("nullfirst", dbDir.toString());
        listener.onDBTruncated("nullfirst", dbDir.toString());

        assertTrue("Stale object must remain until provider recovers",
                   store.fileExists("nullfirst/db/000001.sst"));

        CloudStorageProviderFactory.setActiveProviderForTest(store);
        boolean purged = awaitCondition(
                () -> !store.fileExists("nullfirst/db/000001.sst"), 12_000);
        assertTrue("Initial null-provider truncate must still purge once provider returns", purged);
    }

    @Test
    public void delete_providerUnavailable_marksPendingAndBlocksHydrationUntilCleanup()
            throws Exception {
        // Deleting while the provider is unavailable must still leave a durable anti-resurrection
        // guard (a local pending-delete marker) that blocks re-hydration until remote cleanup is
        // confirmed once a provider returns. Unique prefix so any late retry cannot touch other tests.
        Path dbDir = mkdirs("provdown/db");
        store.put("provdown/db/000001.sst", bytes("stale"));
        store.put("provdown/db/CURRENT", bytes("MANIFEST-000001"));

        CloudStorageEventListener listener = listenerFor(dataRoot);
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);

        // Delete while the provider is unavailable.
        CloudStorageProviderFactory.setActiveProviderForTest(null);
        listener.onDBDeleteBegin("provdown/db", dbDir.toString());
        listener.onDBDeleted("provdown/db", dbDir.toString());

        assertTrue("A pending-delete marker must guard the delete during provider outage",
                   markerDirHasEntry(markerDir));

        // Re-open while still unavailable: hydration stays blocked (marker persists, no exception).
        listener.onDBOpening("provdown/db", dbDir.toString());
        assertTrue("Marker must persist (hydration blocked) while cleanup is unconfirmed",
                   markerDirHasEntry(markerDir));

        // Provider returns: opening completes the cleanup inline (purge + marker removal).
        CloudStorageProviderFactory.setActiveProviderForTest(store);
        listener.onDBOpening("provdown/db", dbDir.toString());

        assertFalse("Marker must be cleared once remote cleanup completes",
                    markerDirHasEntry(markerDir));
        assertFalse("Stale remote object must be purged on cleanup",
                    store.fileExists("provdown/db/000001.sst"));
    }

    @Test
    public void delete_purgeFailure_retriesUntilCleanupCompletes() throws Exception {
        // Same safety level as truncate: a failed delete purge must be retried until the remote
        // prefix is purged and the local marker cleared.
        Path dbDir = mkdirs("delfail/db");
        store.put("delfail/db/000001.sst", bytes("stale"));

        CloudStorageEventListener listener = listenerFor(dataRoot);
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);

        // First purge attempt fails.
        store.setPrefixDeleteFailure(true);
        listener.onDBDeleteBegin("delfail/db", dbDir.toString());
        listener.onDBDeleted("delfail/db", dbDir.toString());

        assertTrue("Marker must remain while purge is failing", markerDirHasEntry(markerDir));
        assertTrue("Stale object remains after a failed purge",
                   store.fileExists("delfail/db/000001.sst"));

        // Recovery: purge now succeeds; the scheduled retry must purge and clear the marker.
        store.setPrefixDeleteFailure(false);
        boolean cleaned = awaitCondition(
                () -> !store.fileExists("delfail/db/000001.sst") && !markerDirHasEntry(markerDir),
                8_000);
        assertTrue("Delete purge must retry until the stale prefix is purged and the marker cleared",
                   cleaned);
    }

    @Test
    public void startupPendingDeleteMarker_validMarker_isReplayedAndPurged() throws Exception {
        // Simulate a crash after writing the local marker but before the remote purge completed.
        store.put("startup/db/000001.sst", bytes("stale"));
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);
        Files.createDirectories(markerDir);

        String prefix = "startup/db";
        String markerName = java.util.Base64.getUrlEncoder().withoutPadding()
                                             .encodeToString(prefix.getBytes(StandardCharsets.UTF_8));
        Files.write(markerDir.resolve(markerName), prefix.getBytes(StandardCharsets.UTF_8));

        // New listener instance => startup marker scan + deferred purge scheduling.
        listenerFor(dataRoot);

        boolean cleaned = awaitCondition(
                () -> !store.fileExists("startup/db/000001.sst") && !markerDirHasEntry(markerDir),
                8_000);
        assertTrue("A valid startup marker must trigger purge replay and marker cleanup", cleaned);
    }

    @Test
    public void startupPendingDeleteMarker_mismatchedFilename_isRejected() throws Exception {
        store.put("badmarker/db/000001.sst", bytes("stale"));
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);
        Files.createDirectories(markerDir);

        String payloadPrefix = "badmarker/db";
        String wrongEncodedName = java.util.Base64.getUrlEncoder().withoutPadding()
                                           .encodeToString("other/db".getBytes(StandardCharsets.UTF_8));
        Files.write(markerDir.resolve(wrongEncodedName),
                    payloadPrefix.getBytes(StandardCharsets.UTF_8));

        listenerFor(dataRoot);

        // No purge should be scheduled from an invalid marker.
        Thread.sleep(700L);
        assertTrue("Invalid startup marker must not trigger remote purge",
                   store.fileExists("badmarker/db/000001.sst"));
        assertTrue("Invalid marker should remain for explicit operator inspection",
                   markerDirHasEntry(markerDir));
    }

    @Test
    public void startupPendingTruncateMarker_validMarker_isReplayedAndPurged() throws Exception {
        // Simulate a crash after truncate completed locally but remote purge intent remained pending.
        store.put("truncstart/db/000001.sst", bytes("stale"));
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_TRUNCATE_DIR);
        Files.createDirectories(markerDir);

        String prefix = "truncstart/db";
        String markerName = java.util.Base64.getUrlEncoder().withoutPadding()
                                             .encodeToString(prefix.getBytes(StandardCharsets.UTF_8));
        Files.write(markerDir.resolve(markerName), prefix.getBytes(StandardCharsets.UTF_8));

        // New listener instance => startup marker scan + deferred truncate purge scheduling.
        listenerFor(dataRoot);

        boolean cleaned = awaitCondition(
                () -> !store.fileExists("truncstart/db/000001.sst") && !markerDirHasEntry(markerDir),
                10_000);
        assertTrue("A valid startup truncate marker must trigger purge replay and marker cleanup",
                   cleaned);
    }

    /** True if {@code dir} exists and contains at least one entry. */
    private static boolean markerDirHasEntry(Path dir) {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        try (Stream<Path> s = Files.list(dir)) {
            return s.findAny().isPresent();
        } catch (IOException e) {
            return false;
        }
    }

    // =========================================================================
    // 17c. Post-upload metadata sync is debounced (coalesced) per DB
    // =========================================================================

    @Test
    public void metadataSync_debounced_coalescesBurstThenPublishesTrailing() throws Exception {
        AtomicInteger syncCount = new AtomicInteger(0);
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), 0) {
            @Override
            boolean syncMetadataSnapshotInline(CloudStorageProvider p, String db) {
                syncCount.incrementAndGet();
                return true;
            }
        };
        listener.setMetadataSyncDebounceMs(300L);

        // A burst of 5 "post-upload" sync requests inside one window must collapse to a single
        // immediate (leading-edge) publish — the other 4 only arm one trailing sync.
        for (int i = 0; i < 5; i++) {
            listener.requestDebouncedMetadataSync(store, "hugegraph");
        }
        assertEquals("Burst within the debounce window must publish exactly once (leading edge)",
                     1, syncCount.get());

        // The trailing sync must eventually fire so the final state is published even though the
        // burst was coalesced.
        boolean trailing = awaitCondition(() -> syncCount.get() >= 2, 3_000);
        assertTrue("A trailing metadata sync must publish the final state after the window",
                   trailing);
        assertEquals("Exactly one trailing sync must fire for the coalesced burst",
                     2, syncCount.get());

        // After the window has elapsed, a fresh request publishes immediately again (leading edge).
        Thread.sleep(350L);
        listener.requestDebouncedMetadataSync(store, "hugegraph");
        assertEquals("A request after the window must publish immediately",
                     3, syncCount.get());
    }

    // =========================================================================
    // 17d. Metadata-sync backlog bound forces a publish by count (RPO by count)
    // =========================================================================

    @Test
    public void metadataSync_backlogBound_forcesPublishByCountWithinWindow() throws Exception {
        Path tempDir = mkdirs("hugegraph/meta-tmp");
        // A minimal non-null snapshot; uploadMetadataSnapshot is stubbed so its files are unused.
        MetadataSnapshot fake = new MetadataSnapshot(
                tempDir.toString(), tempDir.toString(), "CURRENT", "MANIFEST-000001",
                Collections.emptyList(), Collections.emptyList(), 1L);

        AtomicInteger publishes = new AtomicInteger(0);
        CloudStorageEventListener listener = createListener(fake, publishes);

        // 9 uploads: request 1 publishes (leading edge); the count bound then forces a publish
        // every 3rd unmirrored upload (requests 4 and 7); requests 8-9 stay coalesced.
        for (int i = 0; i < 9; i++) {
            listener.requestDebouncedMetadataSync(store, "hugegraph");
        }

        assertEquals("count bound must force a publish every 3 unmirrored uploads plus the leading "
                     + "publish (requests 1, 4, 7)", 3, publishes.get());
    }

    private @NotNull CloudStorageEventListener createListener(MetadataSnapshot fake,
                                                              AtomicInteger publishes) {
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), 0) {
            @Override
            MetadataSnapshot captureMetadataSnapshot(String db) {
                return fake;
            }

            @Override
            boolean uploadMetadataSnapshot(CloudStorageProvider p, String db, MetadataSnapshot s) {
                publishes.incrementAndGet();
                return true;
            }
        };
        // Make the time window effectively never elapse on its own, isolating the COUNT bound.
        listener.setMetadataSyncDebounceMs(60_000L);
        listener.setMetadataSyncMaxUnpublished(3);
        return listener;
    }

    // =========================================================================
    // 18. Backpressure slows caller when pending backlog exceeds watermark
    // =========================================================================

    @Test
    public void backpressure_slowsCallerWhenBacklogExceedsWatermark() throws Exception {
        // Latch that holds every upload until we release it, so the backlog stays elevated.
        CountDownLatch uploadBlocked = new CountDownLatch(1);
        FakeCloudStore slowStore = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                try { uploadBlocked.await(10, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                super.uploadFile(localPath, remoteKey);
            }
        };
        CloudStorageProviderFactory.setActiveProviderForTest(slowStore);

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                3, 10L, 100L, dataRoot.toString(),
                tracker::markConfirmedIfEpoch);

        // Watermark of 1: a single in-flight upload should trigger backpressure.
        int watermark = 1;
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, retryQueue, tracker, watermark);

        Path dbDir = mkdirs("hugegraph/db");

        // First SST: queued in the upload executor; its upload is blocked by the latch.
        Path sst1 = writeSst(dbDir, "000001.sst", "data-1");
        listener.onTableFileCreated("hugegraph", "default", sst1.toString(), Files.size(sst1));

        // Second SST: must trigger backpressure because the executor queue is non-empty.
        // Measure how long the call takes — it should block for at least BACKPRESSURE_POLL_MS.
        Path sst2 = writeSst(dbDir, "000002.sst", "data-2");
        long t0 = System.nanoTime();
        listener.onTableFileCreated("hugegraph", "default", sst2.toString(), Files.size(sst2));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        // Release the blocked uploads before any assertions so cleanup never hangs.
        uploadBlocked.countDown();
        retryQueue.close();

        // The call must have waited at least one backpressure poll interval (50 ms), proving
        // that the throttle actually engaged.  We use a conservative lower bound (30 ms) to
        // accommodate CI scheduling jitter while still catching the case where backpressure
        // is completely removed.
        assertTrue("onTableFileCreated must block under backpressure; elapsed=" + elapsedMs + "ms",
                   elapsedMs >= 30L);
    }

    @Test
    public void backpressure_ignoresHistoricalDlqDepth() throws Exception {
        // A large DLQ is historical debt (uploads that exhausted their retries), not active lag.
        // It must NOT throttle ingestion — otherwise a node stays degraded long after the provider
        // recovered. Only active work (executor queue/active + retry in-flight) may apply backpressure.
        CloudSyncTracker tracker = new CloudSyncTracker();
        // maxAttempts=0 routes every submit straight to the DLQ; in-flight stays 0.
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                0, 10L, 100L, dataRoot.toString(),
                tracker::markConfirmedIfEpoch);
        retryQueue.setMaxDlqSize(1000);
        for (int i = 0; i < 50; i++) {
            retryQueue.submit("hugegraph", "default",
                              dataRoot.resolve("dlq-" + i + ".sst").toString(),
                              "hugegraph/db/dlq-" + i + ".sst", new IOException("outage"));
        }
        assertTrue("DLQ must be populated to exceed the watermark", retryQueue.getDlqSize() >= 50);

        // Watermark far below the DLQ depth: pre-fix, this would throttle every write for up to the
        // 30s max-wait. The default fast `store` provider keeps active lag ~0.
        int watermark = 5;
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()),
                true, 0L, retryQueue, tracker, watermark);

        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "data");

        long t0 = System.nanoTime();
        listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        retryQueue.close();

        assertTrue("A large historical DLQ must not throttle ingestion; elapsed=" + elapsedMs + "ms",
                   elapsedMs < 5_000L);
    }

    @Test
    public void shutdown_handsOffQueuedUploadTasksToDlq() throws Exception {
        // Saturate the shared upload executor with blocking uploads so later dispatches sit QUEUED,
        // then force a shutdownNow(): the queued tasks must be handed off to the retry queue's DLQ
        // rather than silently dropped from the durability pipeline.
        CountDownLatch block = new CountDownLatch(1);
        FakeCloudStore blockingStore = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                try {
                    block.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted during shutdown");
                }
                super.uploadFile(localPath, remoteKey);
            }
        };
        CloudStorageProviderFactory.setActiveProviderForTest(blockingStore);

        CloudSyncTracker tracker = new CloudSyncTracker();
        // maxAttempts=0 => handed-off tasks land directly in the DLQ where the test can observe them.
        CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                0, 10L, 100L, dataRoot.toString(),
                tracker::markConfirmedIfEpoch);
        // Backpressure disabled (watermark 0) so the producing thread never blocks.
        CloudStorageEventListener listener = new CloudStorageEventListener(
                Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker, 0);

        Path dbDir = mkdirs("hugegraph/db");
        // More SSTs than upload threads: the first few block the workers, the rest queue.
        for (int i = 0; i < 6; i++) {
            Path sst = writeSst(dbDir, String.format("%06d.sst", i), "data-" + i);
            listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));
        }

        // Force shutdownNow quickly: blocked workers cannot finish in 1ms, so queued tasks are
        // dropped by the executor and must be handed off.
        CloudStorageEventListener.shutdownSharedUploadExecutor(1, TimeUnit.MILLISECONDS);
        block.countDown();

        boolean handed = awaitCondition(() -> retryQueue.getDlqSize() > 0, 5_000);
        assertTrue("Queued upload tasks dropped at shutdown must be handed off to the DLQ", handed);
        retryQueue.close();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private CloudStorageEventListener listenerFor(Path root) {
        return listenerFor(Collections.singletonList(root.toString()));
    }

    private CloudStorageEventListener listenerFor(List<String> roots) {
        return new CloudStorageEventListener(roots, true, 0L, null,
                                             new CloudSyncTracker(), 0);
    }

    private Path mkdirs(String relative) throws Exception {
        Path dir = dataRoot.resolve(relative);
        Files.createDirectories(dir);
        return dir;
    }

    private static Path writeSst(Path dir, String name, String content) throws Exception {
        Path sst = dir.resolve(name);
        Files.write(sst, content.getBytes(StandardCharsets.UTF_8));
        return sst;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void awaitUpload(FakeCloudStore store)
            throws Exception {
        long deadline = System.currentTimeMillis() + (long) 2000;
        while (store.totalUploads() < 1) {
            if (System.currentTimeMillis() > deadline) {
                fail("Timed out waiting for " + 1 + " upload(s)");
            }
            Thread.sleep(20);
        }
    }

    private static boolean awaitCondition(BooleanSupplier cond, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    private static void assertNoHydTmpFiles(Path root) throws Exception {
        try (Stream<Path> stream = Files.walk(root)) {
            List<Path> stale = stream
                    // Production names temp files ".hyd-tmp-<threadId>-<nanoTime>" (pre-hydration)
                    // and ".hydrate-<threadId>-<nanoTime>" (read-miss). A plain endsWith(".hyd-tmp")
                    // would match NEITHER, so use a substring match to actually catch leftovers.
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.contains(".hyd-tmp") || name.contains(".hydrate");
                    })
                    .collect(Collectors.toList());
            assertTrue("No stale hydration temp files must remain: " + stale, stale.isEmpty());
        }
    }

    /** Returns the last index of a key whose filename component equals {@code name}. */
    private static int lastIndexOf(List<String> keys, String name) {
        for (int i = keys.size() - 1; i >= 0; i--) {
            String base = Paths.get(keys.get(i)).getFileName().toString();
            if (base.equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static MetadataSnapshot snapshot(Path dbDir, Path tempDir,
                                             String manifestName, long generation)
            throws Exception {
        if (!Files.exists(tempDir.resolve(manifestName))) {
            Files.write(tempDir.resolve(manifestName),
                        ("manifest-gen-" + generation).getBytes(StandardCharsets.UTF_8));
        }
        if (!Files.exists(tempDir.resolve("CURRENT"))) {
            Files.write(tempDir.resolve("CURRENT"),
                        manifestName.getBytes(StandardCharsets.UTF_8));
        }
        return new MetadataSnapshot(
                dbDir.toString(),
                tempDir.toString(),
                "CURRENT",
                manifestName,
                Collections.emptyList(),
                Collections.emptyList(),
                generation);
    }

    // =========================================================================
    // FakeCloudStore — in-memory S3-compatible cloud provider
    // =========================================================================

    /**
     * Thread-safe in-memory implementation of {@link CloudStorageProvider} that stands in for
     * real object storage in all integration tests.  Notable capabilities:
     * <ul>
     *   <li>Faithful {@link #downloadFile}: writes to the supplied path, matching real S3 behaviour.</li>
     *   <li>{@link #deletePrefix}: supports injecting partial per-key errors to exercise the
     *       error-inspection logic in the production {@code S3CloudStorageProvider}.</li>
     *   <li>{@link #setPrefixDeleteFailure}: makes the entire {@code deletePrefix} throw, used to
     *       test that the tombstone is not deleted when the purge fails.</li>
     * </ul>
     */
    static class FakeCloudStore implements CloudStorageProvider {

        private final ConcurrentHashMap<String, byte[]> objects = new ConcurrentHashMap<>();
        private final AtomicInteger uploadCount = new AtomicInteger(0);
        private volatile boolean prefixDeleteFailure = false;
        private volatile List<String> partialDeleteErrors = Collections.emptyList();

        /** Seed a key directly (bypasses the upload counter). */
        void put(String key, byte[] value) {
            objects.put(key, value);
        }

        /** Make the next {@code deletePrefix} call throw an {@link IOException}. */
        void setPrefixDeleteFailure(boolean fail) {
            this.prefixDeleteFailure = fail;
        }

        /**
         * Specify keys whose individual deletion should be reported as a per-key error
         * (simulating S3's partial-failure response in DeleteObjects).
         */
        void setPartialDeleteErrors(List<String> failKeys) {
            this.partialDeleteErrors = failKeys;
        }

        int totalUploads() {
            return uploadCount.get();
        }

        /** Returns cloud CURRENT content as UTF-8, or {@code null} if CURRENT is absent. */
        String readCurrentUtf8() {
            byte[] content = objects.get("hugegraph/db/CURRENT");
            return content == null ? null : new String(content, StandardCharsets.UTF_8);
        }

        @Override
        public String providerName() {
            return "fake";
        }

        @Override
        public void init(CloudStorageConfig config) {
            // no-op
        }

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            byte[] content = Files.readAllBytes(Paths.get(localPath));
            objects.put(remoteKey, content);
            uploadCount.incrementAndGet();
        }

        @Override
        public void deleteFile(String remoteKey) throws IOException {
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
            Files.createDirectories(dest.getParent());
            Files.write(dest, content);
        }

        /**
         * Deletes all objects under the prefix. Supports injecting partial per-key failures
         * (simulating S3 HTTP 200 with per-key Errors in the response body) and a wholesale
         * failure (simulating a network error before any deletion).
         */
        @Override
        public int deletePrefix(String remoteDirPrefix) throws IOException {
            if (prefixDeleteFailure) {
                throw new IOException("Simulated deletePrefix failure for " + remoteDirPrefix);
            }
            String prefix = remoteDirPrefix.endsWith("/") ? remoteDirPrefix : remoteDirPrefix + "/";
            List<String> keys = objects.keySet().stream()
                                       .filter(k -> k.startsWith(prefix))
                                       .collect(Collectors.toList());
            List<String> failed = new ArrayList<>();
            int deleted = 0;
            for (String key : keys) {
                if (partialDeleteErrors.contains(key)) {
                    failed.add(key);
                } else {
                    objects.remove(key);
                    deleted++;
                }
            }
            if (!failed.isEmpty()) {
                throw new IOException("Partial delete failure: " + failed + " could not be deleted");
            }
            return deleted;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    // =========================================================================
    // OrderedRecordingStore — records upload key order for sequencing assertions
    // =========================================================================

    /**
     * Wraps a {@link FakeCloudStore} and captures the order in which remote keys are uploaded.
     * Used by {@link #metadata_publishOrder_currentLastAfterSsts}.
     */
    static class OrderedRecordingStore extends FakeCloudStore {

        private final FakeCloudStore delegate;
        private final List<String> order = new CopyOnWriteArrayList<>();

        OrderedRecordingStore(FakeCloudStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void uploadFile(String localPath, String remoteKey) throws IOException {
            delegate.uploadFile(localPath, remoteKey);
            // Record only the filename component so assertions are path-prefix-agnostic.
            order.add(Paths.get(remoteKey).getFileName().toString());
        }

        @Override
        public boolean fileExists(String remoteKey) {
            return delegate.fileExists(remoteKey);
        }

        @Override
        public List<String> listFiles(String prefix) {
            return delegate.listFiles(prefix);
        }

        @Override
        public void deleteFile(String remoteKey) throws IOException {
            delegate.deleteFile(remoteKey);
        }

        List<String> uploadOrder() {
            return Collections.unmodifiableList(order);
        }
    }

}
