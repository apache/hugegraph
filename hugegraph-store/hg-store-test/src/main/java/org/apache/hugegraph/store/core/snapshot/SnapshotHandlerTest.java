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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.Message;

public class SnapshotHandlerTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * When state is doing (compaction in progress), onSnapshotSave must throw
     * immediately. The exception signals jRaft, which will retry the snapshot later.
     * jRaft's snapshot scheduler runs independently and frequently (default 300s, user config 1800s),
     * so the next snapshot attempt will succeed after compaction completes.
     */
    @Test
    public void testOnSnapshotSaveThrowsWhenCompactionInProgress() {
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
        assertTrue("Exception message must mention compaction is in progress",
                   ex.getMessage().contains("compaction in progress"));
    }

    /**
     * When state is NOT doing (e.g. compactionDone or null), onSnapshotSave must not throw.
     * It should proceed and call saveSnapshot with concrete path verification.
     */
    @Test
    public void testOnSnapshotSaveCallsSaveSnapshotWhenNotBusy() throws Exception {
        PartitionEngine mockEngine = mock(PartitionEngine.class);
        HgStoreEngine mockStoreEngine = mock(HgStoreEngine.class);
        BusinessHandler mockBusinessHandler = mock(BusinessHandler.class);

        final String snapshotPath = tmpDir.newFolder("snap-not-busy").getAbsolutePath();

        // state == compactionDone (not doing) — save should proceed immediately
        AtomicInteger doneState = new AtomicInteger(BusinessHandler.compactionDone);

        when(mockEngine.getGroupId()).thenReturn(0);
        when(mockEngine.getStoreEngine()).thenReturn(mockStoreEngine);
        when(mockStoreEngine.getBusinessHandler()).thenReturn(mockBusinessHandler);
        when(mockBusinessHandler.getState(0)).thenReturn(doneState);

        SnapshotHandler handler = new SnapshotHandler(mockEngine);
        SnapshotWriter stubWriter = stubWriter(snapshotPath);

        handler.onSnapshotSave(stubWriter);

        // Verify: saveSnapshot was called with concrete path containing expected data dir
        String expectedDataDir = snapshotPath + File.separator + "data";
        verify(mockBusinessHandler).saveSnapshot(
                contains(expectedDataDir),  // Must contain the snapshot path + /data
                eq(""),                     // graphName (empty string)
                eq(0));                     // groupId (partition 0)
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
}
