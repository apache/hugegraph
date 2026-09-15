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

package org.apache.hugegraph.store.core.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.consts.PoolNames;
import org.apache.hugegraph.store.core.StoreEngineTestBase;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.snapshot.HgSnapshotHandler;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.Message;

public class HgSnapshotHandlerTest extends StoreEngineTestBase {

    private static HgSnapshotHandler hgSnapshotHandlerUnderTest;

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        hgSnapshotHandlerUnderTest = new HgSnapshotHandler(createPartitionEngine(0));
        FileUtils.forceMkdir(new File("/tmp/snapshot"));
        FileUtils.forceMkdir(new File("/tmp/snapshot/data"));
    }

    @Test
    public void testGetPartitions() {
        // Run the test
        final Map<String, Partition> result = hgSnapshotHandlerUnderTest.getPartitions();
        // Verify the results
        assertEquals(1, result.size());
    }

    @Test
    public void testOnSnapshotSaveAndLoad() {
        String path = "/tmp/snapshot";
        // Setup
        final SnapshotWriter writer = new SnapshotWriter() {
            @Override
            public boolean saveMeta(RaftOutter.SnapshotMeta meta) {
                return false;
            }

            @Override
            public boolean addFile(String fileName, Message fileMeta) {
                return false;
            }

            @Override
            public boolean removeFile(String fileName) {
                return false;
            }

            @Override
            public void close(boolean keepDataOnError) throws IOException {

            }

            @Override
            public boolean init(Void opts) {
                return false;
            }

            @Override
            public void shutdown() {

            }

            @Override
            public String getPath() {
                return path;
            }

            @Override
            public Set<String> listFiles() {
                return null;
            }

            @Override
            public Message getFileMeta(String fileName) {
                return null;
            }

            @Override
            public void close() throws IOException {

            }
        };

        // Run the test
        hgSnapshotHandlerUnderTest.onSnapshotSave(writer);

        // Verify the results

        // Setup
        final SnapshotReader reader = new SnapshotReader() {
            final String path = "/tmp/snapshot";

            @Override
            public RaftOutter.SnapshotMeta load() {
                return null;
            }

            @Override
            public String generateURIForCopy() {
                return null;
            }

            @Override
            public boolean init(Void opts) {
                return false;
            }

            @Override
            public void shutdown() {

            }

            @Override
            public String getPath() {
                return path;
            }

            @Override
            public Set<String> listFiles() {
                return null;
            }

            @Override
            public Message getFileMeta(String fileName) {
                return null;
            }

            @Override
            public void close() throws IOException {

            }
        };

        // Run the test
        hgSnapshotHandlerUnderTest.onSnapshotLoad(reader, 0L);
    }


    @Test
    public void testTrimStartPath() {
        assertEquals("str", HgSnapshotHandler.trimStartPath("str", "prefix"));
    }

    @Test
    public void testFindFileList() {
        // Setup
        final File dir = new File("filename.txt");
        final File rootDir = new File("filename.txt");

        // Run the test
        HgSnapshotHandler.findFileList(dir, rootDir, List.of("value"));

        // Verify the results
    }

    /**
     * Test that onSnapshotLoad skips loading (rather than throwing) when should_not_load is
     * present but data/ is missing: a locally-saved snapshot deliberately has no data/ dir
     * since nothing was meant to load, and should_not_load is checked before the data/ dir.
     */
    @Test
    public void testOnSnapshotLoadSkipsWhenShouldNotLoadPresentButDataMissing()
            throws Exception {
        // Arrange: snapshot dir has should_not_load but NO data/ subdirectory.
        File snapDir = tmpDir.newFolder("snapshot-corrupt");
        File shouldNotLoad = new File(snapDir, "should_not_load");
        Files.write(shouldNotLoad.toPath(), "saved snapshot".getBytes(StandardCharsets.UTF_8));
        // data/ deliberately not created

        SnapshotHandler handler = new SnapshotHandler(createPartitionEngine(1));
        SnapshotReader stubReader = stubReader(snapDir.getAbsolutePath());

        // Must not throw; should return early at the should_not_load check.
        handler.onSnapshotLoad(stubReader, 0L);
    }

    /**
     * Test that onSnapshotLoad skips loading when snapshot is locally saved (both flags present).
     */
    @Test
    public void testOnSnapshotLoadSkipsWhenShouldNotLoadPresentAndDataExists() throws Exception {
        // Arrange: a healthy local snapshot, both should_not_load and data/ present.
        File snapDir = tmpDir.newFolder("snapshot-healthy");
        File shouldNotLoad = new File(snapDir, "should_not_load");
        Files.write(shouldNotLoad.toPath(), "saved snapshot".getBytes(StandardCharsets.UTF_8));
        FileUtils.forceMkdir(new File(snapDir, "data"));

        SnapshotHandler handler = new SnapshotHandler(createPartitionEngine(2));
        SnapshotReader stubReader = stubReader(snapDir.getAbsolutePath());

        // Must not throw; should return early at the should_not_load + data-exists check.
        handler.onSnapshotLoad(stubReader, 0L);
    }

    /**
     * Test that the compaction-range lock used by onSnapshotSave to guard against a concurrent
     * compactRange() call is mutually exclusive and releasable, using the real BusinessHandlerImpl
     * rather than a mock, so the actual lock instance backing the check is exercised. The
     * concurrent attempt runs on a separate thread because the lock is a ReentrantLock: the
     * owning thread can always re-acquire it, so checking from the same thread would not
     * exercise exclusion. In production dbCompaction() and onSnapshotSave() run on different
     * executor threads, which is what this mirrors.
     */
    @Test
    public void testCompactionRangeLockIsMutuallyExclusiveAndReleasable() throws InterruptedException {
        BusinessHandler businessHandler = getStoreEngine().getBusinessHandler();
        int partitionId = 3;

        assertEquals("first reservation must succeed", true,
                     businessHandler.tryLockCompactionRange(partitionId));

        AtomicBoolean concurrentResult = new AtomicBoolean();
        Thread other = new Thread(
                () -> concurrentResult.set(businessHandler.tryLockCompactionRange(partitionId)));
        other.start();
        other.join();
        assertEquals("a concurrent reservation from another thread must fail while the first " +
                     "is held", false, concurrentResult.get());

        businessHandler.unlockCompactionRange(partitionId);

        assertEquals("reservation must succeed again once released", true,
                     businessHandler.tryLockCompactionRange(partitionId));
        businessHandler.unlockCompactionRange(partitionId);
    }

    /**
     * Test that dbCompaction() gives up and skips its pass, rather than blocking forever, when
     * a snapshot save is still holding compactionRangeLock after the configured wait. Shortens
     * compactionRangeLockWaitMillis for the duration of the test so it does not have to wait out
     * the real production timeout, and restores it afterward so other tests are unaffected.
     */
    @Test
    public void testDbCompactionSkipsWhenRangeLockStillHeldAfterWait() throws InterruptedException {
        BusinessHandler businessHandler = getStoreEngine().getBusinessHandler();
        int partitionId = 4;
        createPartitionEngine(partitionId);
        long originalWaitMillis = BusinessHandlerImpl.getCompactionRangeLockWaitMillis();
        BusinessHandlerImpl.setCompactionRangeLockWaitMillis(200);
        try {
            // Simulate a snapshot save that is still in progress.
            assertTrue("snapshot save must reserve the range lock",
                       businessHandler.tryLockCompactionRange(partitionId));

            businessHandler.dbCompaction("graph0", partitionId);

            // dbCompaction() runs on compactionPool asynchronously; give it time to hit the
            // shortened wait and skip, then confirm it never reached the compacting state
            // (doing = -1, set right after the range lock would have been acquired).
            Thread.sleep(1000);
            assertEquals("dbCompaction must never reach the compacting state while the range " +
                         "lock is held by the snapshot save", 0,
                         businessHandler.getState(partitionId).get());

            // The range lock must still belong to the snapshot save - dbCompaction skipping
            // must not have released a lock it never acquired. Check from another thread since
            // the lock is a ReentrantLock and the owning (main) thread could always re-acquire it.
            AtomicBoolean concurrentResult = new AtomicBoolean();
            Thread other = new Thread(() -> concurrentResult.set(
                    businessHandler.tryLockCompactionRange(partitionId)));
            other.start();
            other.join();
            assertFalse("a concurrent reservation attempt must still fail", concurrentResult.get());
        } finally {
            businessHandler.unlockCompactionRange(partitionId);
            BusinessHandlerImpl.setCompactionRangeLockWaitMillis(originalWaitMillis);
        }
    }

    /**
     * Test that dbCompaction() releases the path lock when it is interrupted while waiting
     * on compactionRangeLock, rather than leaking it. Before the fix, an InterruptedException
     * thrown out of rangeLock.tryLock() propagated straight to the outer catch (which only
     * logs), skipping unlock(path) - so every later compaction for that partition would block
     * until the 6-hour path-lock timeout. Interrupts a real compactionPool worker thread while
     * it is parked in tryLock() (identified by stack trace, since the pool is shared), then
     * confirms a second dbCompaction() call is able to complete instead of hanging behind the
     * still-held path lock.
     */
    @Test
    public void testDbCompactionReleasesPathLockWhenInterruptedWaitingForRangeLock()
            throws InterruptedException {
        BusinessHandler businessHandler = getStoreEngine().getBusinessHandler();
        int partitionId = 5;
        createPartitionEngine(partitionId);
        long originalWaitMillis = BusinessHandlerImpl.getCompactionRangeLockWaitMillis();
        // Long enough that the worker thread is still parked in tryLock() when interrupted,
        // rather than racing a real timeout.
        BusinessHandlerImpl.setCompactionRangeLockWaitMillis(60_000);
        try {
            // Simulate a snapshot save that is still in progress, forcing dbCompaction() onto
            // the tryLock(wait) path rather than acquiring the range lock immediately.
            assertTrue("snapshot save must reserve the range lock",
                       businessHandler.tryLockCompactionRange(partitionId));

            businessHandler.dbCompaction("graph0", partitionId);

            // dbCompaction() runs asynchronously on compactionPool; give the submitted task
            // time to acquire the path lock and start waiting on the range lock.
            Thread other = awaitCompactionPoolWorkerBlockedInRangeLockWait();
            assertNotNull("dbCompaction task must be parked in the range lock wait", other);
            other.interrupt();
            // Give the interrupted task time to run its InterruptedException handling and
            // return.
            Thread.sleep(500);

            // The path lock must have been released by the interrupted task's
            // InterruptedException handler, directly confirming the fix rather than relying
            // solely on the second dbCompaction() call below to prove it indirectly.
            String path = businessHandler.getLockPath(partitionId);
            AtomicInteger pathLockState = businessHandler.getPathLockState(path);
            assertNotNull("path lock must have been initialized by the interrupted task",
                           pathLockState);
            assertEquals("path lock must be released, not left in the doing state, after the " +
                         "interrupted task returns",
                         BusinessHandler.compactionCanStart, pathLockState.get());

            // The snapshot save still owns the range lock, unaffected by dbCompaction's
            // interrupt.
            AtomicBoolean concurrentRangeLockResult = new AtomicBoolean();
            Thread rangeLockCheck = new Thread(() -> concurrentRangeLockResult.set(
                    businessHandler.tryLockCompactionRange(partitionId)));
            rangeLockCheck.start();
            rangeLockCheck.join();
            assertFalse("range lock must still belong to the snapshot save",
                        concurrentRangeLockResult.get());

            // The path lock, however, must have been released by the interrupted task -
            // otherwise this second dbCompaction() call would block on lock(path) until the
            // 6-hour timeout instead of reaching the compacting state below once the range
            // lock is released. Ownership of the range lock passes to the second
            // dbCompaction() call's own worker thread, which acquires and releases it itself -
            // do not touch compactionRangeLock again after this point.
            businessHandler.unlockCompactionRange(partitionId);
            businessHandler.dbCompaction("graph0", partitionId);

            // Compaction on the test's near-empty RocksDB completes almost immediately, so the
            // transient "doing" state cannot be reliably observed here - poll for the
            // terminal compactionDone state instead. What this proves is that dbCompaction()
            // was able to acquire lock(path) at all: before the fix, the leaked path lock
            // would have made this call hang on lock(path) until the 6-hour timeout instead
            // of ever reaching compactionDone.
            long start = System.currentTimeMillis();
            while (businessHandler.getState(partitionId).get() != BusinessHandler.compactionDone &&
                   System.currentTimeMillis() - start < 5000) {
                Thread.sleep(50);
            }
            assertEquals("second dbCompaction() must complete, proving the path lock was " +
                         "released rather than leaked by the interrupted task",
                         BusinessHandler.compactionDone, businessHandler.getState(partitionId).get());
        } finally {
            BusinessHandlerImpl.setCompactionRangeLockWaitMillis(originalWaitMillis);
        }
    }

    /**
     * Polls the compactionPool worker threads for one parked inside ReentrantLock#tryLock
     * (the compactionRangeLock wait in dbCompaction()), up to 5s. The pool is shared/static, so
     * this cannot target the task directly - it identifies the right worker by stack trace
     * instead. tryLock(timeout, unit) parks via LockSupport.parkNanos, which reports as
     * TIMED_WAITING rather than WAITING.
     */
    private static Thread awaitCompactionPoolWorkerBlockedInRangeLockWait()
            throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            for (Thread t : Thread.getAllStackTraces().keySet()) {
                if (t.getName().startsWith(PoolNames.COMPACT) &&
                    t.getState() == Thread.State.TIMED_WAITING &&
                    isBlockedInRangeLockTryLock(t)) {
                    return t;
                }
            }
            Thread.sleep(50);
        }
        return null;
    }

    private static boolean isBlockedInRangeLockTryLock(Thread t) {
        boolean inBusinessHandlerImpl = false;
        boolean inLockSupportPark = false;
        for (StackTraceElement frame : t.getStackTrace()) {
            String className = frame.getClassName();
            if (className.equals(BusinessHandlerImpl.class.getName())) {
                inBusinessHandlerImpl = true;
            } else if (className.equals("java.util.concurrent.locks.LockSupport")) {
                inLockSupportPark = true;
            }
        }
        return inBusinessHandlerImpl && inLockSupportPark;
    }

    private static SnapshotReader stubReader(String path) {
        return new SnapshotReader() {
            @Override public RaftOutter.SnapshotMeta load() { return null; }
            @Override public String generateURIForCopy() { return null; }
            @Override public boolean init(Void opts) { return false; }
            @Override public void shutdown() {}
            @Override public String getPath() { return path; }
            @Override public Set<String> listFiles() { return null; }
            @Override public Message getFileMeta(String fileName) { return null; }
            @Override public void close() {}
        };
    }
}
