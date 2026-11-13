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

package org.apache.hugegraph.store.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.upgrade.LegacyGraphIdChecker;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;

public class GraphIdSnapshotGuardTest {

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void testRejectedIncomingSnapshotNeverOpensLocalDatabase() throws Exception {
        Path incoming = databaseWithPrefix(0xFFFE);
        BusinessHandlerImpl handler = mock(BusinessHandlerImpl.class, CALLS_REAL_METHODS);
        HgStoreException failure = assertThrows(HgStoreException.class,
                                               () -> handler.loadSnapshot(incoming.toString(), "g", 7, 100));
        assertEquals(HgStoreException.EC_RKDB_IMPORT_SNAPSHOT_FAIL, failure.getCode());
        assertTrue(failure.getMessage().contains("0xFFFE"));
        assertTrue(failure.getMessage().contains(incoming.toString()));
        assertTrue(failure.getCause() instanceof IllegalStateException);
        verify(handler, never()).getSession(7);
    }

    @Test
    public void testLegalHighGraphIdReachesExistingSnapshotLoader() throws Exception {
        Path incoming = databaseWithPrefix(0xFFFD);
        BusinessHandlerImpl handler = mock(BusinessHandlerImpl.class, CALLS_REAL_METHODS);
        HgStoreException reachedLoader = new HgStoreException("Reached existing loader");
        doThrow(reachedLoader).when(handler).getSession(7);
        assertSame(reachedLoader, assertThrows(HgStoreException.class,
                                              () -> handler.loadSnapshot(incoming.toString(), "g", 7, 100)));
        verify(handler).getSession(7);
    }

    @Test
    public void testSnapshotIndexIsPublishedOnlyAfterSuccessfulLoad() {
        SnapshotHandler handler = mock(SnapshotHandler.class);
        PartitionStateMachine machine = new PartitionStateMachine(7, handler);
        SnapshotReader reader = reader(42);
        doAnswer(invocation -> {
            assertEquals(0, machine.getCommittedIndex());
            assertEquals(42L, (long) invocation.getArgument(1));
            return null;
        }).when(handler).onSnapshotLoad(any(), anyLong());
        assertTrue(machine.onSnapshotLoad(reader));
        assertEquals(42, machine.getCommittedIndex());
    }

    @Test
    public void testRejectedSnapshotPreservesPreviousCommittedIndex() {
        SnapshotHandler handler = mock(SnapshotHandler.class);
        PartitionStateMachine machine = new PartitionStateMachine(7, handler);
        assertTrue(machine.onSnapshotLoad(reader(42)));
        doThrow(new HgStoreException("Legacy GraphID snapshot")).when(handler)
                                                               .onSnapshotLoad(any(), anyLong());
        assertFalse(machine.onSnapshotLoad(reader(100)));
        assertEquals(42, machine.getCommittedIndex());
    }

    @Test
    public void testUnreadableSnapshotMetadataPreservesCommittedIndex() {
        SnapshotHandler handler = mock(SnapshotHandler.class);
        PartitionStateMachine machine = new PartitionStateMachine(7, handler);
        assertTrue(machine.onSnapshotLoad(reader(42)));
        SnapshotReader broken = mock(SnapshotReader.class);
        when(broken.load()).thenThrow(new IllegalStateException("Unreadable metadata"));
        assertFalse(machine.onSnapshotLoad(broken));
        assertEquals(42, machine.getCommittedIndex());
    }

    private Path databaseWithPrefix(int id) throws Exception {
        RocksDB.loadLibrary();
        RocksDBFactory.getInstance().setHugeConfig(LegacyGraphIdChecker.configuration(Map.of()));
        Path path = this.temporary.newFolder().toPath();
        try (Options options = new Options().setCreateIfMissing(true);
             ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
             RocksDB db = RocksDB.open(options, path.toString());
             ColumnFamilyHandle table = db.createColumnFamily(new ColumnFamilyDescriptor(
                     HugeServerTables.VERTEX_TABLE.getBytes(StandardCharsets.UTF_8), familyOptions))) {
            db.put(table, new byte[]{(byte) (id >>> 8), (byte) id, 0, 0, 1}, new byte[]{1, 2, 3});
        }
        return path;
    }

    private static SnapshotReader reader(long index) {
        SnapshotReader reader = mock(SnapshotReader.class);
        when(reader.load()).thenReturn(SnapshotMeta.newBuilder().setLastIncludedIndex(index)
                                                  .setLastIncludedTerm(3).build());
        return reader;
    }
}
