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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Set;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.Message;

public class SnapshotHandlerTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * When the compaction-range lock cannot be reserved (a compaction is actively running),
     * onSnapshotSave must throw immediately without touching saveSnapshot. The exception signals
     * jRaft, which will retry the snapshot later. jRaft's snapshot scheduler runs independently
     * and frequently (default 300s, user config 1800s), so the next attempt will succeed once
     * compaction releases the lock.
     */
    @Test
    public void testOnSnapshotSaveThrowsWhenCompactionInProgress() {
        PartitionEngine mockEngine = mock(PartitionEngine.class);
        HgStoreEngine mockStoreEngine = mock(HgStoreEngine.class);
        BusinessHandler mockBusinessHandler = mock(BusinessHandler.class);

        when(mockEngine.getGroupId()).thenReturn(0);
        when(mockEngine.getStoreEngine()).thenReturn(mockStoreEngine);
        when(mockStoreEngine.getBusinessHandler()).thenReturn(mockBusinessHandler);
        when(mockBusinessHandler.tryLockCompactionRange(0)).thenReturn(false);

        SnapshotHandler handler = new SnapshotHandler(mockEngine);
        SnapshotWriter stubWriter = stubWriter("/tmp/snapshot");

        HgStoreException ex = assertThrows(
                "onSnapshotSave must throw when the compaction-range lock is held",
                HgStoreException.class,
                () -> handler.onSnapshotSave(stubWriter));

        assertTrue("Exception message must mention the partition",
                   ex.getMessage().contains("0"));
        assertTrue("Exception message must mention compaction is in progress",
                   ex.getMessage().contains("compaction in progress"));
        verify(mockBusinessHandler, never()).saveSnapshot(any(), any(), anyInt());
    }

    /**
     * When the compaction-range lock is free, onSnapshotSave must reserve it, call saveSnapshot,
     * and release the lock afterwards.
     */
    @Test
    public void testOnSnapshotSaveCallsSaveSnapshotWhenNotBusy() throws Exception {
        PartitionEngine mockEngine = mock(PartitionEngine.class);
        HgStoreEngine mockStoreEngine = mock(HgStoreEngine.class);
        BusinessHandler mockBusinessHandler = mock(BusinessHandler.class);

        final String snapshotPath = tmpDir.newFolder("snap-not-busy").getAbsolutePath();

        when(mockEngine.getGroupId()).thenReturn(0);
        when(mockEngine.getStoreEngine()).thenReturn(mockStoreEngine);
        when(mockStoreEngine.getBusinessHandler()).thenReturn(mockBusinessHandler);
        when(mockBusinessHandler.tryLockCompactionRange(0)).thenReturn(true);

        SnapshotHandler handler = new SnapshotHandler(mockEngine);
        SnapshotWriter stubWriter = stubWriter(snapshotPath);

        handler.onSnapshotSave(stubWriter);

        // Verify: saveSnapshot was called with concrete path containing expected data dir
        String expectedDataDir = snapshotPath + File.separator + "data";
        verify(mockBusinessHandler).saveSnapshot(
                contains(expectedDataDir),  // Must contain the snapshot path + /data
                eq(""),                     // graphName (empty string)
                eq(0));                     // groupId (partition 0)
        verify(mockBusinessHandler).unlockCompactionRange(0);
    }

    /**
     * When should_not_load is absent (the common corruption variant: leader crashed
     * mid-checkpoint with no flag ever written) and data/ is missing, onSnapshotLoad
     * must throw a diagnostic naming the corrupt snapshot directory, rather than
     * falling through to businessHandler.loadSnapshot.
     */
    @Test
    public void testOnSnapshotLoadThrowsWhenShouldNotLoadAbsentAndDataMissing() throws Exception {
        PartitionEngine mockEngine = mock(PartitionEngine.class);
        HgStoreEngine mockStoreEngine = mock(HgStoreEngine.class);
        BusinessHandler mockBusinessHandler = mock(BusinessHandler.class);

        when(mockEngine.getGroupId()).thenReturn(3);
        when(mockEngine.getStoreEngine()).thenReturn(mockStoreEngine);
        when(mockStoreEngine.getBusinessHandler()).thenReturn(mockBusinessHandler);

        String snapshotPath = tmpDir.newFolder("snapshot-no-flag-no-data").getAbsolutePath();
        // should_not_load deliberately not created; data/ deliberately not created

        SnapshotHandler handler = new SnapshotHandler(mockEngine);
        SnapshotReader stubReader = stubReader(snapshotPath);

        HgStoreException ex = assertThrows(
                "onSnapshotLoad must throw when data/ is missing, flag or no flag",
                HgStoreException.class,
                () -> handler.onSnapshotLoad(stubReader, 0L));

        assertTrue("Exception message must name the corrupt snapshot directory",
                   ex.getMessage().contains(snapshotPath));
    }

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
}
