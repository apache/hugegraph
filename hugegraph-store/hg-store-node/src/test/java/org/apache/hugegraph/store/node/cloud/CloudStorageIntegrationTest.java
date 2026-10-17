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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 *   <li>{@code retryQueue_exhausted_givenUp} — exhausted retries are counted as given up, not
 *       lost silently</li>
 *   <li>{@code readMiss_hydration_restoresMissingLiveFile} — missing live SST is restored on read miss</li>
 *   <li>{@code readMiss_guardWindow_suppressesRepeatAttempts} — guard window dedups rapid re-hydration</li>
 *   <li>{@code deleteGuard_blocksDeleteUntilLiveSetDurable} — delete is held until replacements are in cloud</li>
 * </ol>
 */
@SuppressWarnings({"BusyWait", "ResultOfMethodCallIgnored"})
public class CloudStorageIntegrationTest {

    @TempDir
    Path tmp;

    private Path dataRoot;
    private FakeCloudStore store;

    @BeforeEach
    public void setUp() throws Exception {
        dataRoot = Files.createDirectory(tmp.resolve("storage"));
        store = new FakeCloudStore();
    }

    // =========================================================================
    // 1. Basic SST upload / download round-trip
    // =========================================================================

    @Test
    public void sst_uploadAndDownload_roundTrip() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "row1=v1;row2=v2");

        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));
        awaitUpload(store);

        // Remote key must be relative to dataRoot.
        String relKey = dataRoot.relativize(sst).toString();
        assertTrue(store.fileExists(relKey), "SST must be uploaded to cloud");

        // Download back to a different path and verify content.
        Path recovered = dataRoot.resolve("recovered.sst");
        store.downloadFile(relKey, recovered.toString());
        assertEquals(Files.readString(sst),
                     Files.readString(recovered),
                     "Recovered content must match original");
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

        CloudStorageEventListener listener = listenerFor(atomicityChecker, dataRoot);
        listener.onDBOpening("hugegraph", dbDir.toString());

        assertTrue(violations.isEmpty(), "Atomicity violations detected: " + violations);

        // All files must be present at their final paths with the correct content.
        assertEquals("sst-body",
                     Files.readString(dataRoot.resolve(sstKey)),
                     "SST content must match after atomic hydration");
        assertEquals("MANIFEST-000001",
                     Files.readString(dataRoot.resolve(currentKey)),
                     "CURRENT content must match after atomic hydration");
        assertEquals("manifest-body",
                     Files.readString(dataRoot.resolve(manifestKey)),
                     "MANIFEST content must match after atomic hydration");

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
        assertTrue(Files.exists(staleTemp), "Stale temp file must exist before hydration");

        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        listener.onDBOpening("hugegraph", dbDir.toString());

        // The stale temp file must have been removed.
        assertFalse(Files.exists(staleTemp),
                    "Stale .hyd-tmp must be removed before fresh download");

        // The correct content must be at the final path.
        assertTrue(Files.exists(dbDir.resolve("000001.sst")), "SST must be hydrated correctly");
        assertEquals("sst-body",
                     Files.readString(dbDir.resolve("000001.sst")));
    }

    // =========================================================================
    // 4. Pre-hydration with multiple data roots uses the correct root
    // =========================================================================

    @Test
    public void preHydration_multiRoot_usesCorrectDataRoot() throws Exception {
        // Two data roots: primary and secondary.
        Path primaryRoot = Files.createDirectory(tmp.resolve("storage0"));
        Path secondaryRoot = Files.createDirectory(tmp.resolve("storage1"));

        // DB lives under the secondary root.
        Path dbDir = secondaryRoot.resolve("hugegraph/db");
        Files.createDirectories(dbDir);

        // The remote key was built by stripping secondaryRoot, so it is "hugegraph/db/000001.sst".
        String sstKey = "hugegraph/db/000001.sst";
        store.put(sstKey, bytes("secondary-sst-body"));
        store.put("hugegraph/db/CURRENT", bytes("MANIFEST-000001"));
        store.put("hugegraph/db/MANIFEST-000001", bytes("manifest"));

        CloudStorageEventListener listener = listenerFor(store,
                Arrays.asList(primaryRoot.toString(), secondaryRoot.toString()));

        listener.onDBOpening("hugegraph", dbDir.toString());

        // File must land under secondaryRoot, not primaryRoot.
        assertTrue(Files.exists(secondaryRoot.resolve(sstKey)),
                   "SST must be hydrated under the matching secondary data root");
        assertFalse(Files.exists(primaryRoot.resolve(sstKey)),
                    "SST must NOT be placed under the primary data root");
    }

    // =========================================================================
    // 5. Tombstone key format: sibling (prefix_DELETED), not child (prefix/_DELETED)
    // =========================================================================

    @Test
    public void tombstone_writtenAsSiblingOfDataPrefix() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        CloudStorageEventListener listener = listenerFor(store, dataRoot);

        listener.onDBDeleteBegin("hugegraph", dbDir.toString());

        // Sibling key must be uploaded.
        String siblingKey = "hugegraph/db" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        assertTrue(store.fileExists(siblingKey),
                   "Tombstone must be written as sibling key '" + siblingKey + "'");

        // Inner key must NOT be uploaded.
        String innerKey = "hugegraph/db/" + CloudStorageEventListener.DB_TOMBSTONE_SUFFIX;
        assertFalse(store.fileExists(innerKey),
                    "Tombstone must NOT be written inside the data prefix '" + innerKey + "'");
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
        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        listener.onDBOpening("hugegraph", dbDir.toString());

        // Stale SST must NOT be downloaded.
        assertFalse(Files.exists(dbDir.resolve("000001.sst")),
                    "Stale SST must not be hydrated when tombstone is present");
        assertFalse(Files.exists(dbDir.resolve("CURRENT")),
                    "Stale CURRENT must not be hydrated when tombstone is present");

        // Stale data prefix must be purged.
        assertFalse(store.fileExists("hugegraph/db/000001.sst"),
                    "Stale SST must be purged from cloud after tombstone detection");
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
        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        listener.onDBDeleted("hugegraph", dbDir.toString());

        // Purge failed → tombstone must still be present in cloud.
        assertTrue(store.fileExists(tombstoneKey),
                   "Tombstone must survive a failed prefix purge");
        // Stale SST must still be present (purge failed).
        assertTrue(store.fileExists("hugegraph/db/000001.sst"),
                   "Stale SST must remain when purge fails");

        // Now a re-open must be blocked by the surviving tombstone: the listener finds the
        // tombstone, tries to purge (still fails), and throws IllegalStateException to fail
        // DB open rather than silently proceeding on empty local state.
        Files.createDirectories(dbDir);
        CloudStorageEventListener listener2 = listenerFor(store, dataRoot);
        try {
            listener2.onDBOpening("hugegraph", dbDir.toString());
            fail("Expected IllegalStateException: purge failure must block DB open");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("purge failed"),
                       "Exception message must describe purge failure");
        }
        assertFalse(Files.exists(dbDir.resolve("000001.sst")),
                    "Stale SST must NOT be hydrated when tombstone-guarded purge fails");
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
        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        listener.onDBDeleted("hugegraph", dbDir.toString());

        // Purge succeeded → tombstone must be cleaned up.
        assertFalse(store.fileExists(tombstoneKey),
                    "Tombstone must be removed after successful purge");
        assertFalse(store.fileExists("hugegraph/db/000001.sst"), "Data SST must be purged");
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
            assertTrue(e.getMessage().contains("000002.sst"),
                       "Error must name the failed key");
        }

        // The two successfully deleted keys must be gone; the failed one must survive.
        assertFalse(store.fileExists("hugegraph/db/000001.sst"), "000001.sst must be deleted");
        assertFalse(store.fileExists("hugegraph/db/000003.sst"), "000003.sst must be deleted");
        assertTrue(store.fileExists("hugegraph/db/000002.sst"),
                   "000002.sst must survive a per-key delete failure");
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
        Files.writeString(tempDir.resolve("OPTIONS-000002"), "options");
        Files.writeString(tempDir.resolve("MANIFEST-000001"),
                          "manifest");
        Files.writeString(tempDir.resolve("CURRENT"), "MANIFEST-000001");

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

        CloudStorageEventListener listener = listenerFor(ordered, dataRoot);
        boolean published = listener.uploadMetadataSnapshot(ordered, "hugegraph", snapshot);

        assertTrue(published, "Metadata publish must succeed");

        List<String> uploadOrder = ordered.uploadOrder();
        // CURRENT must be the last metadata file uploaded.
        int currentIdx  = lastIndexOf(uploadOrder, "CURRENT");
        int manifestIdx = lastIndexOf(uploadOrder, "MANIFEST-000001");
        int sstIdx      = lastIndexOf(uploadOrder, "000001.sst");

        assertTrue(sstIdx < manifestIdx, "SSTs must be uploaded before MANIFEST");
        assertTrue(manifestIdx < currentIdx, "MANIFEST must be uploaded before CURRENT");
    }

    // =========================================================================
    // 11. Stale snapshot rejected by generation check
    // =========================================================================

    @Test
    public void metadata_staleSnapshot_rejectedByGenerationCheck() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path tempDir = mkdirs("hugegraph/db-checkpoint");

        CloudStorageEventListener listener = listenerFor(store, dataRoot);

        // Publish generation 10 to establish the watermark.
        MetadataSnapshot gen10 = snapshot(dbDir, tempDir, "MANIFEST-000010", 10L);
        assertTrue(listener.uploadMetadataSnapshot(store, "hugegraph", gen10),
                   "Generation 10 must publish successfully");

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
        assertTrue(listener.uploadMetadataSnapshot(store, "hugegraph", gen10),
                   "Re-publishing generation 10 must succeed after gen5 direct upload");

        // Build a second listener that has the watermark already at 10 and attempt
        // to push gen5 through syncMetadataSnapshotInline using an override.
        // We use a subclass seam: override captureMetadataSnapshot to supply gen5.
        CloudStorageEventListener guardedListener = new CloudStorageEventListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED) {
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
        assertEquals(uploadsAtWatermark,
                     store.totalUploads(),
                     "No uploads must occur when a stale generation is rejected");
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
        CloudStorageEventListener listener = new CloudStorageEventListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED) {
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

        assertTrue(ready.await(5, TimeUnit.SECONDS), "Workers must be ready in 5s");
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "All threads must finish in 10s");

        assertTrue(errors.isEmpty(), "No concurrent publish errors: " + errors);

        // The highest generation can never be stale, so it always publishes; once it does, every
        // lower generation captured afterwards is rejected by the guard. Whatever the
        // (nondeterministic) lock-acquisition order, the durable CURRENT must therefore point at the
        // MAX generation's MANIFEST — a stale generation can never be the last writer.
        String winningManifest = String.format("MANIFEST-%06d", (long) threads);
        assertTrue(store.fileExists("hugegraph/db/CURRENT"),
                   "CURRENT must be present in cloud after concurrent publishes");
        assertEquals(winningManifest,
                     store.readCurrentUtf8(),
                     "Cloud CURRENT must point at the highest generation's manifest — only the "
                     + "non-stale generation may win");
        assertTrue(store.fileExists("hugegraph/db/" + winningManifest),
                   "The winning manifest object must be durable in cloud");

        long published = publishResults.stream().filter(Boolean::booleanValue).count();
        assertTrue(published >= 1, "At least the max generation must have published");
        assertTrue(published <= threads, "No more successful publishes than offered generations");
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

        CloudSyncTracker tracker = new CloudSyncTracker();
        // Wire the SAME epoch-aware callback production uses (AppConfig wires
        // syncTracker::markConfirmedIfEpoch). This ensures the test fails if the retry path drops
        // the epoch — the plain 2-arg callback would confirm regardless and hide that defect.
        CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(flakyStore,
                3, 10L, 100L,
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
        assertTrue(confirmed, "SST must be confirmed after retry");
        assertTrue(flakyStore.fileExists("hugegraph/db/000001.sst"),
                   "SST must be in cloud after retry");

        retryQueue.close();
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

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(flakyStore,
                3, 10L, 100L,
                tracker::markConfirmedIfEpoch);
        CloudStorageEventListener listener = CloudStorageTestFactory.newListener(flakyStore,
                Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker);

        // Dispatch the async upload: onTableFileCreated creates the staged pin, then the first
        // attempt fails and (with the fix) hands the PIN to the retry queue.
        listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));

        // Simulate compaction deleting the original SST before the retry fires. The pin (a hard link)
        // keeps the byteset alive, so the retry must still succeed.
        Files.deleteIfExists(sst);
        assertFalse(Files.exists(sst), "Original SST must be gone (compacted) before the retry");

        boolean confirmed = awaitCondition(
                () -> tracker.isConfirmed("hugegraph", sst.toString()), 5_000);
        assertTrue(confirmed,
                   "Retry must upload from the staged pin and confirm even after the original SST "
                   + "is deleted");
        assertTrue(flakyStore.fileExists("hugegraph/db/000001.sst"),
                   "SST must be present in cloud after the pin-based retry");

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

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = createRetryQueue(flakyStore, tracker, dbDir, tempDir);

        long epoch = tracker.currentEpoch("hugegraph");
        retryQueue.submit("hugegraph", "default", sst.toString(), "hugegraph/db/000001.sst",
                          epoch, new IOException("initial failure"));

        boolean confirmed = awaitCondition(
                () -> tracker.isConfirmed("hugegraph", sst.toString()), 5_000);
        assertTrue(confirmed, "SST must be confirmed after retry");

        // The retry success must ALSO trigger a metadata publish so cloud CURRENT catches up.
        boolean published = awaitCondition(
                () -> flakyStore.fileExists("hugegraph/db/CURRENT"), 5_000);
        assertTrue(published,
                   "Retry success must publish CURRENT/MANIFEST (recovery point must advance, not "
                   + "just the tracker confirmation)");
        assertEquals("MANIFEST-000005",
                     flakyStore.readCurrentUtf8(),
                     "Published CURRENT must reference the captured generation's manifest");

        retryQueue.close();
    }

    private @NotNull CloudUploadRetryQueue createRetryQueue(CloudStorageProvider provider,
                                                            CloudSyncTracker tracker, Path dbDir,
                                                            Path tempDir) {
        CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(provider,
                3, 10L, 100L,
                tracker::markConfirmedIfEpoch);

        CloudStorageEventListener listener = new CloudStorageEventListener(provider,
                Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker,
                null, CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED) {
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
    // 14. Retry queue gives up (and counts it) when all retries exhausted
    // =========================================================================

    @Test
    public void retryQueue_exhausted_givenUp() throws Exception {
        Path dbDir = mkdirs("hugegraph/db");
        Path sst = writeSst(dbDir, "000001.sst", "data");

        // Always fail.
        FakeCloudStore alwaysFail = new FakeCloudStore() {
            @Override
            public void uploadFile(String localPath, String remoteKey) throws IOException {
                throw new IOException("permanent failure");
            }
        };

        CloudSyncTracker tracker = new CloudSyncTracker();
        CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(alwaysFail,
                2, 10L, 50L,
                tracker::markConfirmed);

        retryQueue.submit("hugegraph", "default", sst.toString(),
                          "hugegraph/db/000001.sst", new IOException("initial failure"));

        // Wait until the queue gives up on the upload.
        boolean givenUp = awaitCondition(() -> retryQueue.getGivenUpCount() > 0, 5_000);
        assertTrue(givenUp, "Exhausted upload must be counted as given up");
        assertFalse(tracker.isConfirmed("hugegraph", sst.toString()),
                    "File must not be confirmed when given up");

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
        CloudStorageEventListener listener = CloudStorageTestFactory.newListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, tracker);

        List<LiveSstFile> liveFiles = Collections.singletonList(
                new LiveSstFile(sst.toString(), "default"));

        int restored = listener.restoreMissingLiveFiles(store, "hugegraph", liveFiles);

        assertEquals(1, restored, "One live SST must be restored");
        assertTrue(Files.exists(sst), "Restored SST must exist at original path");
        assertEquals("row1=v1;row2=v2",
                     Files.readString(sst));
        assertTrue(tracker.isConfirmed("hugegraph", sst.toString()),
                   "Restored SST must be marked confirmed in tracker");
    }

    // =========================================================================
    // 16. Read-miss guard window suppresses rapid repeated hydration attempts
    // =========================================================================

    @Test
    public void readMiss_guardWindow_suppressesRepeatAttempts() throws Exception {
        long guardWindowMs = 1_000L;
        CloudStorageEventListener listener = CloudStorageTestFactory.newListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, guardWindowMs, null, new CloudSyncTracker());

        // First attempt: allowed.
        assertTrue(listener.shouldAttemptReadMissHydration("hugegraph", "default"),
                   "First read-miss attempt must be allowed");

        // Second attempt within the guard window: suppressed.
        assertFalse(listener.shouldAttemptReadMissHydration("hugegraph", "default"),
                    "Second read-miss within guard window must be suppressed");

        // After the guard window expires: allowed again.
        Thread.sleep(guardWindowMs + 50);
        assertTrue(listener.shouldAttemptReadMissHydration("hugegraph", "default"),
                   "Read-miss attempt after guard window expiry must be allowed");
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

        CloudStorageEventListener listener = CloudStorageTestFactory.newListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, tracker);

        List<LiveSstFile> liveFiles = Arrays.asList(
                new LiveSstFile(sst1.toString(), "default"),
                new LiveSstFile(sst2.toString(), "default"));

        // Live set not fully durable (sst2 unconfirmed and present locally).
        // ensureLiveSetUploaded must upload sst2 and return true.
        boolean durable = listener.ensureLiveSetUploaded(store, "hugegraph", liveFiles);
        assertTrue(durable, "Live set must be durable after ensureLiveSetUploaded");
        assertTrue(store.fileExists("hugegraph/db/000002.sst"),
                   "sst2 must be uploaded to cloud");
        assertTrue(tracker.isConfirmed("hugegraph", sst2.toString()),
                   "sst2 must be marked confirmed after upload");
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

        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        // Shrink the stale-latch window so the test does not wait a real minute.
        listener.setMaxTruncationDurationMsForTest();

        // Begin truncation but never complete it — the latch is now set and, without the fix,
        // would stay set forever.
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        assertTrue(listener.isActivelyTruncating("hugegraph"),
                   "Latch must be active immediately after truncate begins");

        // Wait past the stale-latch window: the leaked latch must self-heal so cloud metadata
        // mirroring, object deletion, and the delete guard resume for this DB.
        Thread.sleep(80L);
        assertFalse(listener.isActivelyTruncating("hugegraph"),
                    "Stale truncating latch must self-heal after the max window (else cloud "
                    + "mirroring/deletes are disabled for this DB forever)");

        // A brand-new truncation still latches correctly (self-heal must not be sticky).
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        assertTrue(listener.isActivelyTruncating("hugegraph"),
                   "A fresh truncate must re-arm the latch");
    }

    @Test
    public void truncateAbort_clearsLatchImmediately_withoutPurgingRemote() throws Exception {
        // Source-level guarantee: RocksDBSession.truncate() now calls notifyTruncateAbort in a
        // finally when drop/create throws, so the listener clears suppression at once — no need to
        // wait for the stale-latch window — and must NOT purge remote (data may still exist locally).
        Path dbDir = mkdirs("hugegraph/db");
        // Seed remote data that a wrongful purge-on-abort would destroy.
        store.put("hugegraph/db/000001.sst", bytes("still-needed"));

        CloudStorageEventListener listener = listenerFor(store, dataRoot);

        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        assertTrue(listener.isActivelyTruncating("hugegraph"),
                   "Latch must be active after truncate begins");

        // Simulate truncate() failing partway → finally fires the abort notification.
        listener.onDBTruncateAbort("hugegraph", dbDir.toString());

        assertFalse(listener.isActivelyTruncating("hugegraph"),
                    "Abort must clear the truncating latch immediately");
        assertFalse(listener.isInTruncationGracePeriodForTest("hugegraph"),
                    "Abort must not leave the DB in the truncation grace period");
        assertTrue(store.fileExists("hugegraph/db/000001.sst"),
                   "Abort must NOT purge remote state (data may still be present locally)");
    }

    @Test
    public void truncatePurgeFailure_retriesUntilRemoteIsPurged() throws Exception {
        // graph.clear() → onDBTruncated must purge the remote prefix. If that purge fails, stale
        // remote objects could resurrect cleared data on a future restore, so the failure must not
        // be ignored: the listener retries the purge until it succeeds.
        Path dbDir = mkdirs("hugegraph/db");
        store.put("hugegraph/db/000001.sst", bytes("cleared-data"));
        store.put("hugegraph/db/CURRENT", bytes("MANIFEST-000001"));

        CloudStorageEventListener listener = listenerFor(store, dataRoot);

        // First purge attempt fails (simulated transient cloud error).
        store.setPrefixDeleteFailure(true);
        listener.onDBTruncateBegin("hugegraph", dbDir.toString());
        listener.onDBTruncated("hugegraph", dbDir.toString());

        assertTrue(store.fileExists("hugegraph/db/000001.sst"),
                   "Stale object must still be present right after a failed purge");

        // Recovery: subsequent purges succeed; the scheduled retry must clean up the stale objects.
        store.setPrefixDeleteFailure(false);
        boolean purged = awaitCondition(
                () -> !store.fileExists("hugegraph/db/000001.sst"), 8_000);
        assertTrue(purged,
                   "A failed truncate purge must be retried until the stale remote prefix "
                   + "is clean");
    }

    @Test
    public void delete_purgeFailing_marksPendingAndBlocksHydrationUntilCleanup()
            throws Exception {
        // Deleting while the remote purge is failing must still leave a durable anti-resurrection
        // guard (a local pending-delete marker) that blocks re-hydration until remote cleanup is
        // confirmed once purges succeed again. Unique prefix so any late retry cannot touch other
        // tests.
        Path dbDir = mkdirs("provdown/db");
        store.put("provdown/db/000001.sst", bytes("stale"));
        store.put("provdown/db/CURRENT", bytes("MANIFEST-000001"));

        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);

        // Delete while the remote purge fails.
        store.setPrefixDeleteFailure(true);
        listener.onDBDeleteBegin("provdown/db", dbDir.toString());
        listener.onDBDeleted("provdown/db", dbDir.toString());

        assertTrue(markerDirHasEntry(markerDir),
                   "A pending-delete marker must guard the delete while the purge is failing");

        // Re-open while the purge still fails: hydration stays blocked (marker persists, no
        // exception).
        listener.onDBOpening("provdown/db", dbDir.toString());
        assertTrue(markerDirHasEntry(markerDir),
                   "Marker must persist (hydration blocked) while cleanup is unconfirmed");

        // Purge recovers: opening completes the cleanup inline (purge + marker removal).
        store.setPrefixDeleteFailure(false);
        listener.onDBOpening("provdown/db", dbDir.toString());

        assertFalse(markerDirHasEntry(markerDir),
                    "Marker must be cleared once remote cleanup completes");
        assertFalse(store.fileExists("provdown/db/000001.sst"),
                    "Stale remote object must be purged on cleanup");
    }

    @Test
    public void delete_purgeFailure_retriesUntilCleanupCompletes() throws Exception {
        // Same safety level as truncate: a failed delete purge must be retried until the remote
        // prefix is purged and the local marker cleared.
        Path dbDir = mkdirs("delfail/db");
        store.put("delfail/db/000001.sst", bytes("stale"));

        CloudStorageEventListener listener = listenerFor(store, dataRoot);
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);

        // First purge attempt fails.
        store.setPrefixDeleteFailure(true);
        listener.onDBDeleteBegin("delfail/db", dbDir.toString());
        listener.onDBDeleted("delfail/db", dbDir.toString());

        assertTrue(markerDirHasEntry(markerDir), "Marker must remain while purge is failing");
        assertTrue(store.fileExists("delfail/db/000001.sst"),
                   "Stale object remains after a failed purge");

        // Recovery: purge now succeeds; the scheduled retry must purge and clear the marker.
        store.setPrefixDeleteFailure(false);
        boolean cleaned = awaitCondition(
                () -> !store.fileExists("delfail/db/000001.sst") && !markerDirHasEntry(markerDir),
                8_000);
        assertTrue(cleaned,
                   "Delete purge must retry until the stale prefix is purged and the marker "
                   + "cleared");
    }

    @Test
    public void startupPendingDeleteMarker_validMarker_isReplayedAndPurged() throws Exception {
        // Simulate a crash after writing the local marker but before the remote purge completed.
        store.put("startup/db/000001.sst", bytes("stale"));
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);
        Files.createDirectories(markerDir);

        // Markers are empty files; the Base64 filename alone carries the prefix.
        String markerName = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString("startup/db".getBytes(StandardCharsets.UTF_8));
        Files.createFile(markerDir.resolve(markerName));

        // New listener instance => startup marker scan + deferred purge scheduling.
        listenerFor(store, dataRoot);

        boolean cleaned = awaitCondition(
                () -> !store.fileExists("startup/db/000001.sst") && !markerDirHasEntry(markerDir),
                8_000);
        assertTrue(cleaned, "A valid startup marker must trigger purge replay and marker cleanup");
    }

    @Test
    public void startupPendingDeleteMarker_undecodableFilename_isRejected() throws Exception {
        store.put("badmarker/db/000001.sst", bytes("stale"));
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_DELETE_DIR);
        Files.createDirectories(markerDir);

        // '!' is not in the URL-safe Base64 alphabet, so the name cannot be decoded to a prefix.
        Files.createFile(markerDir.resolve("not-base64!"));

        listenerFor(store, dataRoot);

        // No purge should be scheduled from an invalid marker.
        Thread.sleep(700L);
        assertTrue(store.fileExists("badmarker/db/000001.sst"),
                   "Invalid startup marker must not trigger remote purge");
        assertTrue(markerDirHasEntry(markerDir),
                   "Invalid marker should remain for explicit operator inspection");
    }

    @Test
    public void startupPendingTruncateMarker_validMarker_isReplayedAndPurged() throws Exception {
        // Simulate a crash after truncate completed locally but remote purge intent remained pending.
        store.put("truncstart/db/000001.sst", bytes("stale"));
        Path markerDir = dataRoot.resolve(CloudStorageEventListener.PENDING_TRUNCATE_DIR);
        Files.createDirectories(markerDir);

        // Markers are empty files; the Base64 filename alone carries the prefix.
        String markerName = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString("truncstart/db".getBytes(StandardCharsets.UTF_8));
        Files.createFile(markerDir.resolve(markerName));

        // New listener instance => startup marker scan + deferred truncate purge scheduling.
        listenerFor(store, dataRoot);

        boolean cleaned = awaitCondition(
                () -> !store.fileExists("truncstart/db/000001.sst") && !markerDirHasEntry(markerDir),
                10_000);
        assertTrue(cleaned,
                   "A valid startup truncate marker must trigger purge replay and marker cleanup");
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
        CloudStorageEventListener listener = new CloudStorageEventListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED) {
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
        assertEquals(1,
                     syncCount.get(),
                     "Burst within the debounce window must publish exactly once (leading edge)");

        // The trailing sync must eventually fire so the final state is published even though the
        // burst was coalesced.
        boolean trailing = awaitCondition(() -> syncCount.get() >= 2, 3_000);
        assertTrue(trailing,
                   "A trailing metadata sync must publish the final state after the window");
        assertEquals(2,
                     syncCount.get(),
                     "Exactly one trailing sync must fire for the coalesced burst");

        // After the window has elapsed, a fresh request publishes immediately again (leading edge).
        Thread.sleep(350L);
        listener.requestDebouncedMetadataSync(store, "hugegraph");
        assertEquals(3,
                     syncCount.get(),
                     "A request after the window must publish immediately");
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

        assertEquals(3,
                     publishes.get(),
                     "count bound must force a publish every 3 unmirrored uploads plus the leading "
                     + "publish (requests 1, 4, 7)");
    }

    private @NotNull CloudStorageEventListener createListener(MetadataSnapshot fake,
                                                              AtomicInteger publishes) {
        CloudStorageEventListener listener = new CloudStorageEventListener(store,
                Collections.singletonList(dataRoot.toString()),
                true, 0L, null, new CloudSyncTracker(), null,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS,
                CloudStorageEventListener.DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED) {
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

    @Test
    public void shutdown_handsOffQueuedUploadTasksToRetryQueue() throws Exception {
        // Saturate the listener's upload executor with blocking uploads so later dispatches sit
        // QUEUED, then force a shutdownNow(): the queued tasks must be handed off to the retry queue
        // (and given up on there) rather than silently dropped from the durability pipeline.
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

        CloudSyncTracker tracker = new CloudSyncTracker();
        // maxAttempts=0 => handed-off tasks are given up immediately, where the test can observe
        // them via getGivenUpCount().
        CloudUploadRetryQueue retryQueue = CloudStorageTestFactory.newRetryQueue(blockingStore,
                0, 10L, 100L,
                tracker::markConfirmedIfEpoch);
        CloudStorageEventListener listener = CloudStorageTestFactory.newListener(blockingStore,
                Collections.singletonList(dataRoot.toString()), true, 0L, retryQueue, tracker);

        Path dbDir = mkdirs("hugegraph/db");
        // More SSTs than upload threads: the first few block the workers, the rest queue.
        for (int i = 0; i < 6; i++) {
            Path sst = writeSst(dbDir, String.format("%06d.sst", i), "data-" + i);
            listener.onTableFileCreated("hugegraph", "default", sst.toString(), Files.size(sst));
        }

        // Force shutdownNow quickly: blocked workers cannot finish in 1ms, so queued tasks are
        // dropped by the executor and must be handed off.
        listener.close(1, TimeUnit.MILLISECONDS);
        block.countDown();

        boolean handed = awaitCondition(() -> retryQueue.getGivenUpCount() > 0, 5_000);
        assertTrue(handed,
                   "Queued upload tasks dropped at shutdown must be handed off to the retry queue");
        retryQueue.close();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private CloudStorageEventListener listenerFor(CloudStorageProvider provider, Path root) {
        return listenerFor(provider, Collections.singletonList(root.toString()));
    }

    private CloudStorageEventListener listenerFor(CloudStorageProvider provider,
                                                  List<String> roots) {
        return CloudStorageTestFactory.newListener(provider, roots, true, 0L, null,
                                                   new CloudSyncTracker());
    }

    private Path mkdirs(String relative) throws Exception {
        Path dir = dataRoot.resolve(relative);
        Files.createDirectories(dir);
        return dir;
    }

    private static Path writeSst(Path dir, String name, String content) throws Exception {
        Path sst = dir.resolve(name);
        Files.writeString(sst, content);
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
                    .toList();
            assertTrue(stale.isEmpty(), "No stale hydration temp files must remain: " + stale);
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
            Files.writeString(tempDir.resolve(manifestName),
                              "manifest-gen-" + generation);
        }
        if (!Files.exists(tempDir.resolve("CURRENT"))) {
            Files.writeString(tempDir.resolve("CURRENT"),
                              manifestName);
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
                                       .toList();
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
