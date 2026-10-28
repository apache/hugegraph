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

package org.apache.hugegraph.store.core.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.GraphIdManager;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.GraphIdPreflightException;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.protobuf.Int64Value;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.core.FSMCallerImpl;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;

public class GraphIdUpgradePreflightTest {

    private static final int PARTITION_ID = 0;
    private static final String DB_NAME = "00000";
    private static final String[] TABLES = {"g+v", "g+oe", "g+ie", "g+index"};
    private static final byte[] VALUE = {42};

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @BeforeClass
    public static void initRocksdb() {
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.write_buffer_size", "1048576");
        config.put("rocksdb.bloom_filter_bits_per_key", "10");
        BusinessHandlerImpl.initRocksdb(config, null);
    }

    @Test
    public void testStoreStartupRejectsLegacyPartitionBeforeRegisteringWithPd() throws Exception {
        Path location = this.temporary.newFolder("store-startup").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, "00000", 1);
        // A clean latest version must not hide affected data in a retained older version.
        writeDatabase(data, "00001_1", 65534);
        writeDatabase(data, "00001_2", 1);
        Map<String, String> before = fileHashes(location);
        Constructor<HgStoreEngine> constructor = HgStoreEngine.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        HgStoreEngine engine = constructor.newInstance();
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(location.toString());
        options.setRocksdbConfig(new HashMap<>(Map.of("rocksdb.write_buffer_size", "1048576",
                                                     "rocksdb.bloom_filter_bits_per_key", "10")));
        // No PD/RPC endpoint or worker configuration is needed to reject local legacy data.
        GraphIdPreflightException failure = assertThrows(GraphIdPreflightException.class,
                                                        () -> engine.init(options));

        assertTrue(failure.getMessage(), failure.getMessage().contains("startup"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("partition 00001"));
        assertTrue(failure.getMessage(), failure.getMessage().contains("g+v"));
        assertTrue(failure.getMessage(), failure.getMessage().contains(data.resolve("00001_1").toString()));
        assertNull(engine.getPdProvider());
        assertNull(engine.getRaftRpcServer());
        assertNull(engine.getHeartbeatService());
        assertNull(engine.getPartitionManager());
        assertEquals(before, fileHashes(location));
        assertFalse(RocksDBFactory.getInstance().getGraphNames().contains("00000"));
        assertFalse(RocksDBFactory.getInstance().getGraphNames().contains("00001"));
    }

    @Test
    public void testStoreStartupAcceptsCleanPartitionsInAllDataRootsWithoutWrites() throws Exception {
        Path first = this.temporary.newFolder("store-clean-first").toPath();
        Path second = this.temporary.newFolder("store-clean-second").toPath();
        Path firstData = Files.createDirectory(first.resolve("db"));
        Path secondData = Files.createDirectory(second.resolve("db"));
        writeDatabase(firstData, "00000_1", 65533);
        writeDatabase(firstData, "00000_2", 1);
        writeDatabase(secondData, "00001", 2);
        Map<String, String> firstBefore = fileHashes(first);
        Map<String, String> secondBefore = fileHashes(second);

        GraphIdManager.checkStoreDataBeforeStartup(first + "," + second + "," + first.resolve("new-store"));

        assertEquals(firstBefore, fileHashes(first));
        assertEquals(secondBefore, fileHashes(second));
        assertFalse(Files.exists(first.resolve("new-store")));
    }

    @Test
    public void testStoreStartupRejectsNativeFailureWithoutChangingFiles() throws Exception {
        Path location = this.temporary.newFolder("store-corrupt").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, "00001", 1);
        Files.writeString(data.resolve("00001/CURRENT"), "invalid-current");
        Map<String, String> before = fileHashes(location);

        GraphIdPreflightException failure = assertThrows(GraphIdPreflightException.class,
                () -> GraphIdManager.checkStoreDataBeforeStartup(location.toString()));

        assertTrue(failure.getMessage(), failure.getMessage().contains("startup"));
        assertTrue(failure.getCause().getCause() instanceof RocksDBException);
        assertEquals(before, fileHashes(location));
    }

    @Test
    public void testStoreStartupRejectsUnlistableDirectoryAndIncompleteLatestVersion() throws Exception {
        Path location = this.temporary.newFolder("store-unlistable").toPath();
        Path data = Files.writeString(location.resolve("db"), "not a directory");
        assertThrows(GraphIdPreflightException.class,
                     () -> GraphIdManager.checkStoreDataBeforeStartup(location.toString()));
        assertEquals("not a directory", Files.readString(data));
        Files.delete(data);
        Files.createDirectory(data);
        writeDatabase(data, "00001_1", 1);
        Files.createDirectory(data.resolve("00001_2"));
        Map<String, String> before = fileHashes(location);
        assertThrows(GraphIdPreflightException.class,
                     () -> GraphIdManager.checkStoreDataBeforeStartup(location.toString()));
        assertEquals(before, fileHashes(location));
    }

    @Test
    public void testStoreStartupRejectsAnUninspectablePartitionEntry() throws Exception {
        Path location = this.temporary.newFolder("store-uninspectable-partition").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        Files.createSymbolicLink(data.resolve("00001"), location.resolve("missing-target"));
        assertThrows(GraphIdPreflightException.class,
                     () -> GraphIdManager.checkStoreDataBeforeStartup(location.toString()));
    }

    @Test
    public void testNativePreflightFailurePreservesCauseAndFiles() throws Exception {
        Path data = this.temporary.newFolder("native-preflight-failure").toPath();
        writeDatabase(data, DB_NAME, 1);
        Path database = data.resolve(DB_NAME);
        Files.writeString(database.resolve("CURRENT"), "invalid-current");
        Map<String, String> before = fileHashes(data);
        RocksDBException nativeFailure;
        try (Options options = new Options()) {
            nativeFailure = assertThrows(RocksDBException.class, () -> {
                try (RocksDB ignored = RocksDB.openReadOnly(options, database.toString())) {
                    // Close any unexpectedly successful open before reporting failure.
                }
            });
        }
        HgStoreException rejected = assertThrows(HgStoreException.class, () ->
                GraphIdManager.checkLegacyGraphIdData(database.toString(), PARTITION_ID));
        assertEquals(HgStoreException.EC_FAIL, rejected.getCode());
        assertEquals(before, fileHashes(data));
        assertTrue("Expected native cause " + nativeFailure.getStatus().getCode() +
                   ", got " + rejected.getCause(),
                   rejected.getCause() instanceof RocksDBException);
        assertEquals(nativeFailure.getStatus().getCode(),
                     ((RocksDBException) rejected.getCause()).getStatus().getCode());
    }

    @Test
    public void testEffectiveOptionsPreserveLevelSevenBootstrap() throws Exception {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        HugeConfig previous = factory.getHugeConfig();
        try {
            useEightLevelConfiguration();
            Path location = this.temporary.newFolder("eight-level-bootstrap").toPath();
            Path data = Files.createDirectory(location.resolve("db"));
            writeEightLevelDatabase(data.resolve(DB_NAME + "_1"), 1);
            // The normal session can read this legal configuration before the preflight.
            try (RocksDBSession normal = session(data, DB_NAME)) {
                assertRows(normal, 1);
            }
            Map<String, String> before = fileHashes(data);
            GraphIdManager.checkLegacyGraphIdData(data.resolve(DB_NAME + "_1").toString(), PARTITION_ID);
            assertEquals(before, fileHashes(data));
            try (Bootstrap bootstrap = new Bootstrap(location)) {
                bootstrap.manager.loadPartition();
                assertTrue(bootstrap.manager.hasPartition("test", PARTITION_ID));
                assertEquals(1, bootstrap.handler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
                for (String table : TABLES) {
                    assertArrayEquals(VALUE, bootstrap.handler.doGet("test", 0, table, new byte[]{1}));
                }
                GraphIdManager ids = new GraphIdManager(bootstrap.handler::getSession, PARTITION_ID);
                assertTrue(ids.getGraphIdOrCreate("additional") < 65534L);
            }
        } finally {
            factory.setHugeConfig(previous);
        }
    }

    @Test
    public void testEffectiveOptionsRejectLegacyLevelSevenData() throws Exception {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        HugeConfig previous = factory.getHugeConfig();
        try {
            useEightLevelConfiguration();
            Path location = this.temporary.newFolder("eight-level-legacy").toPath();
            Path data = Files.createDirectory(location.resolve("db"));
            writeEightLevelDatabase(data.resolve(DB_NAME + "_1"), 65534);
            Map<String, String> before = fileHashes(data);
            HgStoreException rejected = assertThrows(HgStoreException.class, () ->
                    GraphIdManager.checkLegacyGraphIdData(data.resolve(DB_NAME + "_1").toString(), PARTITION_ID));
            assertTrue(rejected.getMessage().contains("prefix 0xFFFE"));
            assertEquals(before, fileHashes(data));
            try (Bootstrap bootstrap = new Bootstrap(location)) {
                assertThrows(HgStoreException.class, bootstrap.manager::loadPartition);
                assertEquals(before, fileHashes(data));
                assertFalse(factory.getGraphNames().contains(DB_NAME));
            }
        } finally {
            factory.setHugeConfig(previous);
        }
    }

    @Test
    public void testEffectiveOptionsAcceptLevelSevenSnapshot() throws Exception {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        HugeConfig previous = factory.getHugeConfig();
        try {
            useEightLevelConfiguration();
            Path seed = this.temporary.newFolder("eight-level-seed").toPath();
            writeEightLevelDatabase(seed.resolve("seed"), 2);
            Path snapshot = seed.resolve("snapshot");
            try (RocksDBSession source = session(seed, "seed");
                 Checkpoint checkpoint = Checkpoint.create(source.getDB())) {
                checkpoint.createCheckpoint(snapshot.toString());
            }
            Map<String, String> incoming = fileHashes(snapshot);
            Path location = this.temporary.newFolder("eight-level-restore").toPath();
            Path data = Files.createDirectory(location.resolve("db"));
            writeEightLevelDatabase(data.resolve(DB_NAME + "_1"), 1);
            try (Bootstrap bootstrap = new Bootstrap(location)) {
                bootstrap.handler.loadSnapshot(snapshot.toString(), "", PARTITION_ID, 5L);
                try (RocksDBSession restored = bootstrap.handler.getSession(PARTITION_ID)) {
                    assertRows(restored, 2);
                    assertEquals(2, bootstrap.handler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
                }
            }
            assertEquals(incoming, fileHashes(snapshot));
        } finally {
            factory.setHugeConfig(previous);
        }
    }

    private static void useEightLevelConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.write_buffer_size", "1048576");
        config.put("rocksdb.bloom_filter_bits_per_key", "10");
        config.put("rocksdb.num_levels", "8");
        config.put("rocksdb.compression_per_level", "[none,none,none,none,none,none,none,none]");
        BusinessHandlerImpl.initRocksdb(config, null);
    }

    private static void writeEightLevelDatabase(Path path, int graphId) throws Exception {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().setNumLevels(8)
                     .setCompressionPerLevel(Collections.nCopies(8, CompressionType.NO_COMPRESSION))
                     .setDisableAutoCompactions(true).setWriteBufferSize(1048576);
             FlushOptions flush = new FlushOptions().setWaitForFlush(true);
             CompactRangeOptions compact = new CompactRangeOptions().setChangeLevel(true).setTargetLevel(7)) {
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            for (String table : TABLES) {
                descriptors.add(new ColumnFamilyDescriptor(table.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                                            cfOptions));
            }
            RocksDB database = null;
            try {
                database = RocksDB.open(options, path.toString(), descriptors, handles);
                for (int i = 1; i < handles.size(); i++) {
                    database.put(handles.get(i), key(graphId), VALUE);
                }
                if (graphId != 65534) {
                    database.put(handles.get(0), MetadataKeyHelper.getGraphIDKey("test"),
                                 Int64Value.of(graphId).toByteArray());
                    database.put(handles.get(0), MetadataKeyHelper.getPartitionKey("test", PARTITION_ID),
                                 Metapb.Partition.newBuilder().setId(PARTITION_ID).setGraphName("test")
                                       .build().toByteArray());
                }
                database.flush(flush, handles);
                for (int i = 1; i < handles.size(); i++) {
                    database.compactRange(handles.get(i), null, null, compact);
                    assertFalse("A real L7 SST is required for this native compatibility regression",
                                database.getColumnFamilyMetaData(handles.get(i)).levels().get(7).files().isEmpty());
                }
            } finally {
                handles.forEach(ColumnFamilyHandle::close);
                if (database != null) {
                    database.close();
                }
            }
        }
    }

    @Test
    public void testBootstrapPreservesOlderDatabaseWhenLatestHasNoCurrent() throws Exception {
        Path location = this.temporary.newFolder("bootstrap").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME + "_1", 1);
        Files.createDirectory(data.resolve(DB_NAME + "_2"));
        Files.write(data.resolve(DB_NAME + "_2/incomplete-copy"), new byte[]{7});
        Map<String, String> before = fileHashes(data);
        try (Bootstrap bootstrap = new Bootstrap(location)) {
            Throwable failure = null;
            try {
                bootstrap.manager.loadPartition();
            } catch (Throwable caught) {
                failure = caught;
            }
            assertNotNull("Bootstrap must reject before creating/pruning native databases", failure);
            assertTrue(failure.getMessage().contains("no CURRENT"));
            assertEquals(before, fileHashes(data));
            assertFalse(RocksDBFactory.getInstance().getGraphNames().contains(DB_NAME));
        }
    }

    @Test
    public void testBootstrapPreservesLegacyOlderVersionBeforePruning() throws Exception {
        Path location = this.temporary.newFolder("bootstrap-older-legacy").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME + "_1", 65534);
        writeDatabase(data, DB_NAME + "_2", 1);
        Map<String, String> before = fileHashes(data);
        try (Bootstrap bootstrap = new Bootstrap(location)) {
            Throwable failure = null;
            try {
                bootstrap.manager.loadPartition();
            } catch (Throwable caught) {
                failure = caught;
            }
            assertNotNull("Pruning must not erase an older legacy database", failure);
            assertTrue(failure.getMessage().contains("GraphID"));
            assertEquals(before, fileHashes(data));
            assertFalse(RocksDBFactory.getInstance().getGraphNames().contains(DB_NAME));
        }
    }

    @Test
    public void testBootstrapPreservesLegacyLatestVersion() throws Exception {
        Path location = this.temporary.newFolder("bootstrap-latest-legacy").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME + "_1", 1);
        writeDatabase(data, DB_NAME + "_2", 65534);
        Map<String, String> before = fileHashes(data);
        try (Bootstrap bootstrap = new Bootstrap(location)) {
            assertThrows(HgStoreException.class, bootstrap.manager::loadPartition);
            assertEquals(before, fileHashes(data));
            assertFalse(RocksDBFactory.getInstance().getGraphNames().contains(DB_NAME));
        }
    }

    @Test
    public void testBootstrapLoadsCurrentValidMappingAndData() throws Exception {
        Path location = this.temporary.newFolder("bootstrap-valid").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME + "_1", 1);
        writeBootstrapMetadata(data.resolve(DB_NAME + "_1"), 1);
        try (Bootstrap bootstrap = new Bootstrap(location)) {
            bootstrap.manager.loadPartition();
            assertTrue(bootstrap.manager.hasPartition("test", PARTITION_ID));
            assertEquals(1, bootstrap.handler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
            for (String table : TABLES) {
                assertArrayEquals(VALUE, bootstrap.handler.doGet("test", 0, table, new byte[]{1}));
            }
        }
    }

    @Test
    public void testFactoryValidatesColdOpenAndReusesCachedSession() throws Exception {
        Path data = this.temporary.newFolder("factory-valid").toPath();
        writeDatabase(data, DB_NAME, 1);
        java.util.concurrent.atomic.AtomicInteger checks = new java.util.concurrent.atomic.AtomicInteger();
        RocksDBFactory factory = RocksDBFactory.getInstance();
        try {
            try (RocksDBSession first = factory.createGraphDB(data.toString(), DB_NAME, 0L, path -> {
                checks.incrementAndGet();
                GraphIdManager.checkLegacyGraphIdData(path, PARTITION_ID);
            });
                 RocksDBSession cached = factory.createGraphDB(data.toString(), DB_NAME, 0L,
                         path -> org.junit.Assert.fail("Cached session must not be revalidated"))) {
                assertEquals(1, checks.get());
                assertSame(first.getDB(), cached.getDB());
                assertRows(cached, 1);
            }
        } finally {
            factory.releaseGraphDB(DB_NAME);
        }
    }

    @Test
    public void testCachedMemtableLegacyDataRejectsBeforeRaft() throws Exception {
        Path location = this.temporary.newFolder("cached-legacy-location").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        RocksDBFactory factory = RocksDBFactory.getInstance();
        try (RocksDBSession cached = factory.createGraphDB(data.toString(), DB_NAME, 0L)) {
            cached.setDisableWAL(true);
            putRows(cached, 65534);
            // A separate RO instance cannot see WAL-disabled writes still in this primary's memtable.
            GraphIdManager.checkLegacyGraphIdData(cached.getDbPath(), PARTITION_ID);
            assertThrows(HgStoreException.class, () -> GraphIdManager.checkLegacyGraphIdData(cached, PARTITION_ID));
            int references = cached.getRefCount();
            assertThrows(GraphIdPreflightException.class,
                         () -> GraphIdManager.checkStoreDataBeforeStartup(location.toString()));
            assertEquals(references, cached.getRefCount());
            PartitionEngine engine = partitionEngine(location, true);
            PartitionEngineOptions options = new PartitionEngineOptions();
            options.setGroupId(PARTITION_ID);

            HgStoreException failure = assertThrows(HgStoreException.class, () -> engine.init(options));

            assertTrue(failure.getMessage().contains("GraphID"));
            assertNull(engine.getRaftNode());
            assertNull(field(engine, "stateMachine"));
            assertNull(engine.getTaskManager());
            assertEquals(references, cached.getRefCount());
            assertTrue(cached.getDB().isOwningHandle());
            assertRows(cached, 65534);
        } finally {
            factory.releaseGraphDB(DB_NAME);
        }
    }

    @Test
    public void testCachedValidSessionPreflightAvoidsPathResolution() throws Exception {
        Path location = this.temporary.newFolder("cached-valid-location").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        RocksDBFactory factory = RocksDBFactory.getInstance();
        try (RocksDBSession cached = factory.createGraphDB(data.toString(), DB_NAME, 0L)) {
            cached.setDisableWAL(true);
            putRows(cached, 1);
            int references = cached.getRefCount();
            PartitionEngine engine = partitionEngine(location, true);
            // Cached native handles already identify the active DB; do not resolve another disk view.
            when(engine.getStoreEngine().getPartitionManager().getStoreMetadata().getPartitionStore(PARTITION_ID))
                    .thenThrow(new AssertionError("Cached preflight must not resolve a DB path"));
            var check = PartitionEngine.class.getDeclaredMethod("checkLegacyGraphIdData", int.class);
            check.setAccessible(true);
            check.invoke(engine, PARTITION_ID);

            assertEquals(references, cached.getRefCount());
            assertTrue(cached.getDB().isOwningHandle());
            assertRows(cached, 1);
        } finally {
            factory.releaseGraphDB(DB_NAME);
        }
    }

    @Test
    public void testNewPartitionDatabaseStillCreatesNormally() throws Exception {
        Path location = this.temporary.newFolder("bootstrap-new").toPath();
        try (Bootstrap bootstrap = new Bootstrap(location);
             RocksDBSession session = bootstrap.handler.getSession(PARTITION_ID)) {
            assertTrue(Files.isRegularFile(Path.of(session.getDbPath(), "CURRENT")));
            assertTrue(session.getDB().isOwningHandle());
        }
    }

    private static void writeBootstrapMetadata(Path databasePath, long graphId) throws Exception {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions(); ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            for (String table : TABLES) {
                descriptors.add(new ColumnFamilyDescriptor(table.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                                            cfOptions));
            }
            try (RocksDB database = RocksDB.open(options, databasePath.toString(), descriptors, handles)) {
                database.put(handles.get(0), MetadataKeyHelper.getGraphIDKey("test"),
                             Int64Value.of(graphId).toByteArray());
                database.put(handles.get(0), MetadataKeyHelper.getPartitionKey("test", PARTITION_ID),
                             Metapb.Partition.newBuilder().setId(PARTITION_ID).setGraphName("test")
                                   .build().toByteArray());
            } finally {
                handles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    @Test
    public void testBootstrapChecksActualRootAfterLocationUpdate() throws Exception {
        Path location = this.temporary.newFolder("bootstrap-relocated").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME + "_1", 1);
        Files.createDirectory(data.resolve(DB_NAME + "_2"));
        Map<String, String> before = fileHashes(data);
        try (Bootstrap bootstrap = new Bootstrap(location, true)) {
            assertThrows(org.apache.hugegraph.rocksdb.access.DBStoreException.class,
                         bootstrap.manager::loadPartition);
            // Location discovery precedes native validation; it is not a startup transaction.
            assertEquals(location.toString(), bootstrap.manager.getStoreMetadata()
                                                              .getPartitionStore(PARTITION_ID)
                                                              .getStoreLocation());
            org.mockito.Mockito.verify(bootstrap.manager.getStoreMetadata())
                               .savePartitionStore(org.mockito.ArgumentMatchers.any());
            assertEquals(before, fileHashes(data));
            assertFalse(RocksDBFactory.getInstance().getGraphNames().contains(DB_NAME));
        }
    }

    private static class Bootstrap implements AutoCloseable {

        private final PartitionManager manager;
        private final BusinessHandlerImpl handler;
        private final Object previousHandler;
        private final Field handlerField;

        private Bootstrap(Path location) throws Exception {
            this(location, false);
        }

        private Bootstrap(Path location, boolean relocate) throws Exception {
            Files.createDirectories(location.resolve("raft/" + DB_NAME + "/log"));
            HgStoreEngineOptions options = new HgStoreEngineOptions();
            options.setDataPath(location.toString());
            options.setRaftPath(location.toString());
            PdProvider provider = mock(PdProvider.class);
            when(provider.getShardGroup(PARTITION_ID)).thenReturn(
                    Metapb.ShardGroup.newBuilder().setId(PARTITION_ID)
                          .addShards(Metapb.Shard.newBuilder().setStoreId(1L).build()).build());
            Metapb.Partition partition = Metapb.Partition.newBuilder().setId(PARTITION_ID)
                                                        .setGraphName("test").build();
            when(provider.getPartitionByID(anyString(), anyInt())).thenReturn(
                    new org.apache.hugegraph.store.meta.Partition(partition));
            when(provider.getPartitionByCode(anyString(), anyInt())).thenReturn(partition);
            this.manager = new PartitionManager(provider, options);
            StoreMetadata metadata = mock(StoreMetadata.class);
            when(metadata.getStore()).thenReturn(new Store(Metapb.Store.newBuilder().setId(1L).build()));
            AtomicReference<Metapb.PartitionStore> saved = new AtomicReference<>(
                    Metapb.PartitionStore.newBuilder().setPartitionId(PARTITION_ID)
                          .setStoreLocation(relocate ? location.resolve("previous").toString() :
                                            location.toString()).build());
            when(metadata.getPartitionStore(PARTITION_ID)).thenAnswer(call -> saved.get());
            org.mockito.Mockito.doAnswer(call -> {
                saved.set(call.getArgument(0));
                return null;
            }).when(metadata).savePartitionStore(org.mockito.ArgumentMatchers.any());
            when(metadata.getPartitionRaft(PARTITION_ID)).thenReturn(
                    Metapb.PartitionRaft.newBuilder().setPartitionId(PARTITION_ID)
                          .setRaftLocation(location.toString()).build());
            when(metadata.getPartitionStoreLocation(PARTITION_ID, DB_NAME))
                    .thenAnswer(call -> saved.get().getStoreLocation());
            Field metadataField = PartitionManager.class.getDeclaredField("storeMetadata");
            metadataField.setAccessible(true);
            metadataField.set(this.manager, metadata);
            this.handler = new BusinessHandlerImpl(this.manager);
            this.handlerField = HgStoreEngine.class.getDeclaredField("businessHandler");
            this.handlerField.setAccessible(true);
            this.previousHandler = this.handlerField.get(HgStoreEngine.getInstance());
            this.handlerField.set(HgStoreEngine.getInstance(), this.handler);
        }

        @Override
        public void close() throws Exception {
            try {
                RocksDBFactory.getInstance().releaseGraphDB(DB_NAME);
            } finally {
                this.handlerField.set(HgStoreEngine.getInstance(), this.previousHandler);
            }
        }
    }

    @Test
    public void testLegacyDataRejectsBeforeRaftAndPreservesVersions() throws Exception {
        Path location = this.temporary.newFolder("existing-location").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME, 1);
        writeDatabase(data, DB_NAME + "_4", 65534);
        Map<String, String> before = fileHashes(location);
        PartitionEngine engine = partitionEngine(location, true);
        PartitionEngineOptions options = new PartitionEngineOptions();
        options.setGroupId(PARTITION_ID);

        HgStoreException failure = assertThrows(HgStoreException.class, () -> engine.init(options));

        assertTrue(failure.getMessage().contains("GraphID"));
        assertEquals(false, field(engine, "started"));
        assertNull(engine.getRaftNode());
        assertNull(field(engine, "stateMachine"));
        assertNull(engine.getTaskManager());
        assertEquals(before, fileHashes(location));
    }

    @Test
    public void testMissingLocationChecksAllConfiguredRootsWithoutWriting() throws Exception {
        Path first = this.temporary.newFolder("first-root").toPath();
        Path second = this.temporary.newFolder("second-root").toPath();
        writeDatabase(Files.createDirectory(first.resolve("db")), DB_NAME, 1);
        writeDatabase(Files.createDirectory(second.resolve("db")), DB_NAME, 65534);
        Map<String, String> firstBefore = fileHashes(first);
        Map<String, String> secondBefore = fileHashes(second);
        PartitionEngine engine = partitionEngine(first, false);
        engine.getStoreEngine().getOption().setDataPath(first + "," + second);
        PartitionEngineOptions options = new PartitionEngineOptions();
        options.setGroupId(PARTITION_ID);

        assertThrows(HgStoreException.class, () -> engine.init(options));

        assertNull(engine.getRaftNode());
        assertEquals(firstBefore, fileHashes(first));
        assertEquals(secondBefore, fileHashes(second));
    }

    @Test
    public void testLegacySnapshotKeepsActiveAndTargetData() throws Exception {
        Path data = this.temporary.newFolder("active-parent").toPath();
        try (RocksDBSession active = session(data, "active")) {
            putRows(active, 1);
            writeDatabase(data, "active_5", 2);
            Path snapshot = snapshot(65534);
            Map<String, String> before = fileHashes(data);
            Map<String, String> incomingBefore = fileHashes(snapshot);
            BusinessHandlerImpl handler = handler(active);
            assertEquals(1, handler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
            Map<?, ?> cached = (Map<?, ?>) field(handler.getKeyCreator(), "graphIdCache");
            Object owner = cached.get(PARTITION_ID);

            HgStoreException failure = assertThrows(HgStoreException.class,
                    () -> handler.loadSnapshot(snapshot.toString(), "", PARTITION_ID, 5L));

            assertTrue(failure.getMessage().contains("GraphID"));
            assertNotNull(failure.getCause());
            assertSame(owner, cached.get(PARTITION_ID));
            assertEquals(before, fileHashes(data));
            assertEquals(incomingBefore, fileHashes(snapshot));
            assertTrue(Files.isDirectory(data.resolve("active_5_temp_")));
            assertRows(active, 1);
            assertArrayEquals(Int64Value.of(1L).toByteArray(), active.sessionOp().get("default",
                    "HUGEGRAPH/GRAPH_ID/test".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
    }

    @Test
    public void testRejectedNativeSnapshotDoesNotPublishEitherIndex() throws Exception {
        assertSnapshotIndices(true);
    }

    @Test
    public void testAcceptedNativeSnapshotPublishesBothIndices() throws Exception {
        assertSnapshotIndices(false);
    }

    private void assertSnapshotIndices(boolean legacy) throws Exception {
        Path data = this.temporary.newFolder("fsm-active").toPath();
        try (RocksDBSession active = session(data, "fsm")) {
            putRows(active, 1);
            Path snapshot = snapshot(legacy ? 65534 : 2);
            Path snapshotRoot = snapshot.getParent();
            BusinessHandlerImpl handler = handler(active);
            HgStoreEngine store = mock(HgStoreEngine.class);
            PartitionEngine engine = mock(PartitionEngine.class);
            when(engine.getStoreEngine()).thenReturn(store);
            when(engine.getGroupId()).thenReturn(PARTITION_ID);
            when(engine.loadPartitionsFromLocalDb()).thenReturn(Collections.emptyList());
            when(store.getBusinessHandler()).thenReturn(handler);
            PartitionStateMachine machine = new PartitionStateMachine(PARTITION_ID,
                                                                       new SnapshotHandler(engine));
            Field committed = PartitionStateMachine.class.getDeclaredField("committedIndex");
            committed.setAccessible(true);
            committed.setLong(machine, 3L);
            SnapshotReader reader = mock(SnapshotReader.class);
            when(reader.getPath()).thenReturn(snapshotRoot.toString());
            when(reader.load()).thenReturn(RaftOutter.SnapshotMeta.newBuilder()
                                                    .setLastIncludedIndex(5L)
                                                    .setLastIncludedTerm(1L).build());
            NodeImpl node = mock(NodeImpl.class);
            when(node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
            when(node.getGroupId()).thenReturn("graph-id-snapshot-test");
            FSMCallerOptions options = new FSMCallerOptions();
            options.setNode(node);
            options.setFsm(machine);
            options.setLogManager(mock(LogManager.class));
            options.setClosureQueue(new ClosureQueueImpl("graph-id-snapshot-test"));
            options.setBootstrapId(new LogId(3L, 1L));
            options.setDisruptorBufferSize(16);
            FSMCallerImpl caller = new FSMCallerImpl();
            assertTrue(caller.init(options));
            AtomicReference<Status> result = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            try {
                assertTrue(caller.onSnapshotLoad(new LoadSnapshotClosure() {
                    @Override
                    public SnapshotReader start() {
                        return reader;
                    }

                    @Override
                    public void run(Status status) {
                        result.set(status);
                        done.countDown();
                    }
                }));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertEquals(!legacy, result.get().isOk());
                assertEquals(legacy ? 3L : 5L, machine.getCommittedIndex());
                assertEquals(legacy ? 3L : 5L, caller.getLastAppliedIndex());
                assertEquals(legacy ? 3L : 5L, caller.getLastCommittedIndex());
                assertEquals(!legacy, Files.exists(snapshotRoot.resolve("should_not_load")));
                if (legacy) {
                    assertRows(active, 1);
                    assertFalse(Files.exists(data.resolve("fsm_5")));
                } else {
                    assertTrue(Files.isDirectory(data.resolve("fsm_5")));
                    try (RocksDBSession restored = session(data, "fsm")) {
                        assertRows(restored, 2);
                        BusinessHandlerImpl restoredHandler = handler(restored);
                        assertEquals(2, restoredHandler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
                        for (String table : TABLES) {
                            assertArrayEquals(VALUE, restoredHandler.doGet("test", 0, table, new byte[]{1}));
                        }
                    }
                }
            } finally {
                caller.shutdown();
                caller.join();
            }
        }
    }

    private PartitionEngine partitionEngine(Path location, boolean persistedLocation) {
        HgStoreEngine store = mock(HgStoreEngine.class);
        PartitionManager manager = mock(PartitionManager.class);
        StoreMetadata metadata = mock(StoreMetadata.class);
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(location.toString());
        when(store.getOption()).thenReturn(options);
        when(store.getPartitionManager()).thenReturn(manager);
        when(manager.getStoreMetadata()).thenReturn(metadata);
        if (persistedLocation) {
            when(metadata.getPartitionStore(PARTITION_ID)).thenReturn(
                    Metapb.PartitionStore.newBuilder().setPartitionId(PARTITION_ID)
                          .setStoreLocation(location.toString()).build());
        }
        return new PartitionEngine(store, mock(ShardGroup.class));
    }

    private static BusinessHandlerImpl handler(RocksDBSession active) {
        PartitionManager manager = mock(PartitionManager.class);
        PdProvider provider = mock(PdProvider.class);
        when(manager.getPdProvider()).thenReturn(provider);
        when(manager.hasPartition(anyString(), anyInt())).thenReturn(true);
        when(provider.getPartitionByCode(anyString(), anyInt())).thenReturn(
                Metapb.Partition.newBuilder().setId(PARTITION_ID).build());
        return new BusinessHandlerImpl(manager) {
            @Override
            public RocksDBSession getSession(int partitionId) {
                return active.clone();
            }
        };
    }

    private Path snapshot(int graphId) throws Exception {
        Path seed = this.temporary.newFolder("snapshot-seed").toPath();
        Path root = this.temporary.newFolder("snapshot-root").toPath();
        Path data = root.resolve("data");
        try (RocksDBSession session = session(seed, "seed")) {
            putRows(session, graphId);
            try (Checkpoint checkpoint = Checkpoint.create(session.getDB())) {
                checkpoint.createCheckpoint(data.toString());
            }
        }
        return data;
    }

    private static void writeDatabase(Path root, String name, int graphId) {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().setWriteBufferSize(1048576)) {
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            for (String table : TABLES) {
                descriptors.add(new ColumnFamilyDescriptor(table.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                                            cfOptions));
            }
            try (RocksDB database = RocksDB.open(options, root.resolve(name).toString(), descriptors, handles)) {
                for (int i = 1; i < handles.size(); i++) {
                    database.put(handles.get(i), key(graphId), VALUE);
                }
            } finally {
                handles.forEach(ColumnFamilyHandle::close);
            }
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static RocksDBSession session(Path root, String name) {
        return new RocksDBSession(org.apache.hugegraph.rocksdb.access.RocksDBFactory
                                .getInstance().getHugeConfig(), root.toString(), name, 0L);
    }

    private static void putRows(RocksDBSession session, int graphId) {
        session.createTables(TABLES);
        SessionOperator operator = session.sessionOp();
        operator.prepare();
        try {
            for (String table : TABLES) {
                operator.put(table, key(graphId), VALUE);
            }
            if (graphId != 65534) {
                operator.put("default", "HUGEGRAPH/GRAPH_ID/test".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                             Int64Value.of(graphId).toByteArray());
            }
            operator.commit();
        } catch (RuntimeException failure) {
            operator.rollback();
            throw failure;
        }
    }

    private static void assertRows(RocksDBSession session, int graphId) {
        for (String table : TABLES) {
            assertArrayEquals(VALUE, session.sessionOp().get(table, key(graphId)));
        }
    }

    private static byte[] key(int graphId) {
        byte[] key = {0, 0, 1, 0, 0};
        Bits.putShort(key, 0, graphId);
        return key;
    }

    private static Map<String, String> fileHashes(Path root) throws Exception {
        Map<String, String> hashes = new LinkedHashMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.filter(Files::isRegularFile)
                                                   .sorted()::iterator) {
                // Rejected loads deliberately retain a new staging directory.
                if (path.toString().contains("_temp_")) {
                    continue;
                }
                hashes.put(root.relativize(path).toString(),
                           Arrays.toString(MessageDigest.getInstance("SHA-256")
                                                        .digest(Files.readAllBytes(path))));
            }
        }
        return hashes;
    }
}
