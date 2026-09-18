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

package org.apache.hugegraph.store.business;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.util.HgStoreException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.MemoryUsageType;

/**
 * Consolidated unit tests for {@link BusinessHandlerImpl}, including the former
 * {@code BusinessHandlerImplExtendedTest} scenarios.
 *
 * <p>Covers static utility methods, partition operations, and core business logic.
 */
public class BusinessHandlerImplTest {

     private static class SessionOverridingBusinessHandler extends BusinessHandlerImpl {

         private final RocksDBSession session;

         SessionOverridingBusinessHandler(PartitionManager partitionManager, RocksDBSession session) {
             super(partitionManager);
             this.session = session;
         }

         @Override
         public RocksDBSession getSession(int partId) throws HgStoreException {
             return this.session;
         }
     }

     /** Reads the private static {@code indexDataSize} field so setter behavior is observable. */
     private static long readIndexDataSize() throws Exception {
         Field field = BusinessHandlerImpl.class.getDeclaredField("indexDataSize");
         field.setAccessible(true);
         return (Long) field.get(null);
     }

     private BusinessHandlerImpl handler;
     private PartitionManager mockPartitionManager;

    @BeforeClass
     public static void setUpClass() {
         // Initialize static resources if needed
     }

     @Before
     public void setUp() {
         mockPartitionManager = mock(PartitionManager.class);
         RocksDBSession mockSession = mock(RocksDBSession.class);

         handler = new BusinessHandlerImpl(mockPartitionManager);

         // Setup common mock behaviors
         when(mockSession.sessionOp()).thenReturn(mock(SessionOperator.class));
     }

     @Test
     public void testFnvHashIsDeterministicAndSensitiveToInput() {
         byte[] keyA = "abc".getBytes(StandardCharsets.UTF_8);
         byte[] keyB = "abd".getBytes(StandardCharsets.UTF_8);

         Long hashA1 = BusinessHandlerImpl.fnvHash(keyA);
         Long hashA2 = BusinessHandlerImpl.fnvHash(keyA);
         Long hashB = BusinessHandlerImpl.fnvHash(keyB);

         assertEquals(hashA1, hashA2);
         assertNotEquals(hashA1, hashB);
     }

     @Test
     public void testFnvHashEmptyInputUsesOffsetBasis() {
         Long hash = BusinessHandlerImpl.fnvHash(new byte[0]);

         assertEquals(Long.valueOf(0xcbf29ce484222325L), hash);
     }

     @Test
     public void testFnvHashIsSensitiveToByteOrder() {
         byte[] key1 = new byte[]{1, 2, 3};
         byte[] key2 = new byte[]{3, 2, 1};

         Long hash1 = BusinessHandlerImpl.fnvHash(key1);
         Long hash2 = BusinessHandlerImpl.fnvHash(key2);

         assertNotEquals(hash1, hash2);
     }

     @Test
     public void testFnvHashLargeInputReturnsHash() {
         byte[] largeInput = new byte[10000];
         for (int i = 0; i < largeInput.length; i++) {
             largeInput[i] = (byte) (i % 256);
         }

         Long hash = BusinessHandlerImpl.fnvHash(largeInput);

         assertNotNull(hash);
     }

     @Test
     public void testGetDbNameFormatsAndCachesPartitionId() {
         String dbName = BusinessHandlerImpl.getDbName(12);

         assertEquals("00012", dbName);
         assertEquals(dbName, BusinessHandlerImpl.getDbName(12));
     }

     @Test
     public void testGetDbNameFormatsPadding() {
         assertEquals("00001", BusinessHandlerImpl.getDbName(1));
         assertEquals("00010", BusinessHandlerImpl.getDbName(10));
         assertEquals("00100", BusinessHandlerImpl.getDbName(100));
         assertEquals("01000", BusinessHandlerImpl.getDbName(1000));
         assertEquals("10000", BusinessHandlerImpl.getDbName(10000));
     }

     @Test
     public void testGetDbNameReturnsCachedValueForSameId() {
         String first = BusinessHandlerImpl.getDbName(999);
         String second = BusinessHandlerImpl.getDbName(999);
         String third = BusinessHandlerImpl.getDbName(999);

         assertEquals(first, second);
         assertEquals(second, third);
         assertEquals("00999", first);
     }

     @Test
     public void testSetIndexDataSizeStoresPositiveValue() throws Exception {
         long original = readIndexDataSize();
         try {
             BusinessHandlerImpl.setIndexDataSize(100 * 1024L);
             assertEquals(100 * 1024L, readIndexDataSize());
         } finally {
             BusinessHandlerImpl.setIndexDataSize(original);
         }
     }

     @Test
     public void testSetIndexDataSizeIgnoresNonPositiveValue() throws Exception {
         long original = readIndexDataSize();
         try {
             BusinessHandlerImpl.setIndexDataSize(64 * 1024L);
             // Zero and negative values are documented no-ops: the previous value must survive.
             BusinessHandlerImpl.setIndexDataSize(0L);
             assertEquals(64 * 1024L, readIndexDataSize());
             BusinessHandlerImpl.setIndexDataSize(-10L);
             assertEquals(64 * 1024L, readIndexDataSize());
         } finally {
             BusinessHandlerImpl.setIndexDataSize(original);
         }
     }

     @Test
     public void testGetCompactionPoolReturnsInitializedPool() {
         var pool = BusinessHandlerImpl.getCompactionPool();

         assertNotNull(pool);
         assertTrue(pool.getCorePoolSize() > 0);
         assertTrue(pool.getMaximumPoolSize() > 0);
     }

     @Test
     public void testGetCompactionPoolReturnsSameInstance() {
         var pool1 = BusinessHandlerImpl.getCompactionPool();
         var pool2 = BusinessHandlerImpl.getCompactionPool();

         assertEquals(pool1, pool2);
     }

     // ========== Tests for doPut/doGet ==========

     @Test
     public void testDoPutInternalErrorWrapsAsHgStoreException() {
         PartitionManager partitionManager = mock(PartitionManager.class);
         PdProvider pdProvider = mock(PdProvider.class);
         when(partitionManager.getPdProvider()).thenReturn(pdProvider);

         Metapb.Partition partition = Metapb.Partition.newBuilder().setId(10).build();
         when(pdProvider.getPartitionByCode("g", 1)).thenReturn(partition);

         RocksDBSession session = mock(RocksDBSession.class);
         SessionOperator op = mock(SessionOperator.class);
         when(session.sessionOp()).thenReturn(op);
         doThrow(new RuntimeException("prepare boom")).when(op).prepare();

         BusinessHandlerImpl localHandler =
                 new SessionOverridingBusinessHandler(partitionManager, session);

         try {
             localHandler.doPut("g", 1, "g+v", new byte[]{1}, new byte[]{2});
             fail("Expected HgStoreException");
         } catch (HgStoreException e) {
             assertEquals(HgStoreException.EC_RKDB_DOPUT_FAIL, e.getCode());
             assertTrue(e.getMessage().contains("prepare boom"));
             verify(op, times(1)).rollback();
         }
     }

     @Test
     public void testDoGetPartitionNotManagedReturnsNull() throws HgStoreException {
         PartitionManager partitionManager = mock(PartitionManager.class);
         PdProvider pdProvider = mock(PdProvider.class);
         when(partitionManager.getPdProvider()).thenReturn(pdProvider);

         Metapb.Partition partition = Metapb.Partition.newBuilder().setId(7).build();
         when(pdProvider.getPartitionByCode("g", 1)).thenReturn(partition);
         // Partition 7 is not managed by this store, so doGet must short-circuit to null without
         // ever opening a RocksDB session.
         when(partitionManager.hasPartition("g", 7)).thenReturn(false);

         RocksDBSession session = mock(RocksDBSession.class);
         BusinessHandlerImpl localHandler =
                 new SessionOverridingBusinessHandler(partitionManager, session);

         assertNull(localHandler.doGet("g", 1, "g+v", new byte[]{1}));
         verify(partitionManager, times(1)).hasPartition("g", 7);
         verify(session, never()).sessionOp();
     }

     // ========== Tests for getLeaderPartitionIds ==========

     @Test
     public void testGetLeaderPartitionIdsDelegatesToPartitionManager() {
         String graph = "test-graph";
         List<Integer> expectedIds = Arrays.asList(1, 2, 3);
         when(mockPartitionManager.getLeaderPartitionIds(graph)).thenReturn(expectedIds);

         List<Integer> result = handler.getLeaderPartitionIds(graph);

         assertEquals(expectedIds, result);
         verify(mockPartitionManager, times(1)).getLeaderPartitionIds(graph);
     }

     @Test
     public void testGetLeaderPartitionIdsReturnsEmptyListWhenNoLeaders() {
         String graph = "empty-graph";
         when(mockPartitionManager.getLeaderPartitionIds(graph)).thenReturn(Collections.emptyList());

         List<Integer> result = handler.getLeaderPartitionIds(graph);

         assertTrue(result.isEmpty());
         verify(mockPartitionManager, times(1)).getLeaderPartitionIds(graph);
     }

     // ========== Tests for getLeaderPartitionIdSet ==========

     @Test
     public void testGetLeaderPartitionIdSetDelegatesToPartitionManager() {
         when(mockPartitionManager.getLeaderPartitionIdSet()).thenReturn(
                 Collections.singleton(1));

         var result = handler.getLeaderPartitionIdSet();

         assertNotNull(result);
         assertTrue(result.contains(1));
         verify(mockPartitionManager, times(1)).getLeaderPartitionIdSet();
     }

     // ========== Tests for Table operations ==========

     @Test
     public void testExistsTableReturnsSessionResult() {
         RocksDBSession session = mock(RocksDBSession.class);
         when(session.tableIsExist("g+v")).thenReturn(true);
         when(session.tableIsExist("missing")).thenReturn(false);

         BusinessHandlerImpl localHandler =
                 new SessionOverridingBusinessHandler(mockPartitionManager, session);

         assertTrue(localHandler.existsTable("g", 1, "g+v"));
         assertFalse(localHandler.existsTable("g", 1, "missing"));
     }

     @Test
     public void testGetTableNamesReturnsSessionTableKeys() {
         Map<String, ColumnFamilyHandle> tableMap = new HashMap<>();
         for (String t : Arrays.asList("g+v", "g+e", "g+index")) {
             tableMap.put(t, mock(ColumnFamilyHandle.class));
         }

         RocksDBSession session = mock(RocksDBSession.class);
         when(session.getTables()).thenReturn(tableMap);
         BusinessHandlerImpl localHandler =
                 new SessionOverridingBusinessHandler(mockPartitionManager, session);

         List<String> names = localHandler.getTableNames("g", 1);

         assertNotNull(names);
         assertEquals(3, names.size());
         assertTrue(names.containsAll(Arrays.asList("g+v", "g+e", "g+index")));
     }

     // ========== Tests for Partition operations ==========

     // ========== Tests for Metric operations ==========

     @Test
     public void testGetApproximateMemoryUsageByTypeReturnsNonNullMap() {
         List<Cache> caches = new ArrayList<>();

         Map<MemoryUsageType, Long> result = handler.getApproximateMemoryUsageByType(caches);

         assertNotNull(result);
         // Empty map on exception is expected behavior
     }

     // ========== Tests for transaction operations ==========

     @Test
     public void testTxBuilderReturnsBuilderBackedBySession() throws HgStoreException {
         RocksDBSession session = mock(RocksDBSession.class);
         // TxBuilderImpl's constructor opens and prepares an operator on the session.
         SessionOperator op = mock(SessionOperator.class);
         when(session.sessionOp()).thenReturn(op);
         BusinessHandlerImpl localHandler =
                 new SessionOverridingBusinessHandler(mockPartitionManager, session);

         assertNotNull(localHandler.txBuilder("g", 1));
         verify(op, times(1)).prepare();
     }

     // ========== Tests for database operations ==========

     @Test
     public void testCloseDbBasicPathIsInvocable() {
         int partId = 1;

         // Verifies the method can be called
         // Full testing requires RocksDBFactory setup
         handler.closeDB(partId);
     }

     @Test
     public void testFlushAllBasicPathIsInvocable() {
         // Verifies the method can be called without throwing
         handler.flushAll();
     }

     @Test
     public void testCloseAllBasicPathIsInvocable() {
         // Verifies the method can be called without throwing
         handler.closeAll();
     }

     @Test
     public void testTruncateClearsGraphRangeWithoutFiringWholePrefixTruncateCallbacks()
             throws Exception {
         RocksDBSession session = mock(RocksDBSession.class);
         SessionOperator op = mock(SessionOperator.class);
         when(session.sessionOp()).thenReturn(op);

         BusinessHandlerImpl localHandler =
                 new SessionOverridingBusinessHandler(mockPartitionManager, session);

         // Replace the real InnerKeyCreator with a mock so getStartKey/getEndKey/delGraphId do not
         // touch RocksDB or the graph-id metadata store.
         InnerKeyCreator mockKeyCreator = mock(InnerKeyCreator.class);
         Field keyCreatorField = BusinessHandlerImpl.class.getDeclaredField("keyCreator");
         keyCreatorField.setAccessible(true);
         keyCreatorField.set(localHandler, mockKeyCreator);

         localHandler.truncate("g", 1);

         // Only this graph's key range is cleared...
         verify(op, times(1)).deleteRange(any(), any());
         // ...and its graph id is released.
         verify(mockKeyCreator, times(1)).delGraphId(1, "g");
         // A partition's RocksDB instance is shared across graphs, so a per-graph clear must NOT
         // read the db identity for — nor fire — the whole-prefix truncate callbacks, which would
         // purge co-tenant graphs' objects from cloud. (A whole-instance RocksDBSession.truncate()
         // remains the only path that fires them.)
         verify(session, never()).getGraphName();
         verify(session, never()).getDbPath();
         // The session opened by truncate() is closed by try-with-resources.
         verify(session, times(1)).close();
     }

     @Test
     public void testGetPartitionIdsDelegatesToPartitionManager() {
         String graph = "test-graph";
         List<Integer> expectedIds = Arrays.asList(1, 2, 3);
         when(mockPartitionManager.getPartitionIds(graph)).thenReturn(expectedIds);

         List<Integer> result = handler.getPartitionIds(graph);

         assertEquals(expectedIds, result);
     }

     // ========== Tests for lock operations ==========

     @Test
     public void testGetLockPathDelegatesToPartitionManager() {
         int partitionId = 1;
         when(mockPartitionManager.getDbDataPath(partitionId))
                 .thenReturn("/data/partition/00001");

         String lockPath = handler.getLockPath(partitionId);

         assertNotNull(lockPath);
         verify(mockPartitionManager, times(1)).getDbDataPath(partitionId);
     }
}

