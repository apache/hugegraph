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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.core.StoreEngineTestBase;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.snapshot.HgSnapshotHandler;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.HgStoreException;
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

    // ── Fix 1: onSnapshotSave must throw when compaction is in progress ────────

    /**
     * Before the fix, onSnapshotSave silently returned when state == doing,
     * causing JRaft to commit an empty snapshot dir with no data/.
     * After the fix it must throw HgStoreException so JRaft retries instead.
     */
    @Test
    public void testOnSnapshotSaveThrowsWhenCompactionInProgress() {
        // Build a SnapshotHandler wired to a mock PartitionEngine whose BusinessHandler
        // reports state == doing (compaction active) for partition 0.
        PartitionEngine mockEngine = mock(PartitionEngine.class);
        HgStoreEngine mockStoreEngine = mock(HgStoreEngine.class);
        BusinessHandler mockBusinessHandler = mock(BusinessHandler.class);

        AtomicInteger doingState = new AtomicInteger(BusinessHandler.doing);

        when(mockEngine.getGroupId()).thenReturn(0);
        when(mockEngine.getStoreEngine()).thenReturn(mockStoreEngine);
        when(mockStoreEngine.getBusinessHandler()).thenReturn(mockBusinessHandler);
        when(mockBusinessHandler.getState(0)).thenReturn(doingState);

        SnapshotHandler handler = new SnapshotHandler(mockEngine);

        SnapshotWriter stubWriter = stubWriter("/tmp/snapshot");

        HgStoreException ex = assertThrows(
                "onSnapshotSave must throw when state == doing",
                HgStoreException.class,
                () -> handler.onSnapshotSave(stubWriter));

        assertTrue("Exception message must mention the partition",
                   ex.getMessage().contains("0"));
        assertTrue("Exception message must describe the cause",
                   ex.getMessage().contains("compaction in progress"));
    }

    /**
     * When state is NOT doing (e.g. compactionDone), onSnapshotSave must not throw.
     */
    @Test
    public void testOnSnapshotSaveDoesNotThrowWhenNotBusy() throws Exception {
        PartitionEngine mockEngine = mock(PartitionEngine.class);
        HgStoreEngine mockStoreEngine = mock(HgStoreEngine.class);
        BusinessHandler mockBusinessHandler = mock(BusinessHandler.class);

        // state == compactionDone (not doing) — save should proceed normally
        AtomicInteger doneState = new AtomicInteger(BusinessHandler.compactionDone);

        when(mockEngine.getGroupId()).thenReturn(0);
        when(mockEngine.getStoreEngine()).thenReturn(mockStoreEngine);
        when(mockStoreEngine.getBusinessHandler()).thenReturn(mockBusinessHandler);
        when(mockBusinessHandler.getState(0)).thenReturn(doneState);

        // saveSnapshot is a no-op via the mock, so we just need it not to throw at the guard
        SnapshotHandler handler = new SnapshotHandler(mockEngine);
        SnapshotWriter stubWriter = stubWriter(tmpDir.newFolder("snap-not-busy").getAbsolutePath());

        // No exception should propagate from the state guard.
        // (saveSnapshot will throw because the mock returns null for it — that's fine,
        //  we only care the doing-check is not hit.)
        try {
            handler.onSnapshotSave(stubWriter);
        } catch (HgStoreException e) {
            assertFalse("Must not be the compaction-busy exception",
                        e.getMessage().contains("compaction in progress"));
        }
    }

    // ── Fix 2: onSnapshotLoad must not silently skip a corrupt snapshot ────────

    /**
     * Before the fix, onSnapshotLoad returned silently when should_not_load was present,
     * even if data/ was missing — leaving the partition in an undefined state.
     * After the fix it must fall through to the real load path and throw,
     * so JRaft can signal the error and the leader can install a fresh snapshot.
     */
    @Test
    public void testOnSnapshotLoadFallsThroughWhenShouldNotLoadPresentButDataMissing()
            throws Exception {
        // Arrange: snapshot dir has should_not_load but NO data/ subdirectory.
        File snapDir = tmpDir.newFolder("snapshot-corrupt");
        File shouldNotLoad = new File(snapDir, "should_not_load");
        Files.write(shouldNotLoad.toPath(), "saved snapshot".getBytes(StandardCharsets.UTF_8));
        // data/ deliberately not created

        SnapshotHandler handler = new SnapshotHandler(createPartitionEngine(1));
        SnapshotReader stubReader = stubReader(snapDir.getAbsolutePath());

        // The fix causes execution to fall through shouldNotLoad() and call
        // businessHandler.loadSnapshot(missingDataDir) which throws HgStoreException.
        assertThrows(
                "onSnapshotLoad must throw when should_not_load present but data/ missing",
                HgStoreException.class,
                () -> handler.onSnapshotLoad(stubReader, 0L));
    }

    /**
     * When should_not_load is present AND data/ also exists, onSnapshotLoad must
     * return early (normal locally-saved snapshot — no load needed).
     */
    @Test
    public void testOnSnapshotLoadSkipsWhenShouldNotLoadPresentAndDataExists() throws Exception {
        // Arrange: a healthy local snapshot — both should_not_load and data/ present.
        File snapDir = tmpDir.newFolder("snapshot-healthy");
        File shouldNotLoad = new File(snapDir, "should_not_load");
        Files.write(shouldNotLoad.toPath(), "saved snapshot".getBytes(StandardCharsets.UTF_8));
        FileUtils.forceMkdir(new File(snapDir, "data"));

        SnapshotHandler handler = new SnapshotHandler(createPartitionEngine(2));
        SnapshotReader stubReader = stubReader(snapDir.getAbsolutePath());

        // Must not throw — should return early at the should_not_load + data-exists check.
        handler.onSnapshotLoad(stubReader, 0L);
    }

    // ── Stub helpers ──────────────────────────────────────────────────────────

    private static SnapshotWriter stubWriter(String path) {
        return new SnapshotWriter() {
            @Override public boolean saveMeta(RaftOutter.SnapshotMeta meta) { return false; }
            @Override public boolean addFile(String fileName, Message fileMeta) { return false; }
            @Override public boolean removeFile(String fileName) { return false; }
            @Override public void close(boolean keepDataOnError) {}
            @Override public boolean init(Void opts) { return false; }
            @Override public void shutdown() {}
            @Override public String getPath() { return path; }
            @Override public Set<String> listFiles() { return null; }
            @Override public Message getFileMeta(String fileName) { return null; }
            @Override public void close() {}
        };
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
}
