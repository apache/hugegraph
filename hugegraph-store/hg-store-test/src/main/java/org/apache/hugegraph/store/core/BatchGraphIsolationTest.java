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

import static org.apache.hugegraph.store.constant.HugeServerTables.TABLES_MAP;
import static org.apache.hugegraph.store.constant.HugeServerTables.VERTEX_TABLE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.store.UnitTestBase;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.business.DataManagerImpl;
import org.apache.hugegraph.store.business.DefaultDataMover;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.OpType;
import org.apache.hugegraph.store.grpc.session.BatchEntry;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.RaftRocksdbOptions;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;
import org.apache.hugegraph.store.pd.PdProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.google.protobuf.ByteString;

public class BatchGraphIsolationTest {

    private static final int PARTITION_ID = 0;
    private static final int EMPTY_PARTITION_ID = 1;
    private static final int KEY_CODE = 0;
    private static final int EMPTY_PARTITION_KEY_CODE = 32768;
    private static final byte[] SHARED_KEY =
            "shared-key".getBytes(StandardCharsets.UTF_8);

    private static Path databasePath;
    private static BusinessHandler handler;

    @BeforeClass
    public static void setup() throws IOException {
        databasePath = Files.createTempDirectory("hugegraph-batch-graph-isolation-");

        Map<String, Object> rocksdbConfig = new HashMap<>();
        rocksdbConfig.put("rocksdb.write_buffer_size", "1048576");
        StorageOptionsFactory.releaseAllOptions();
        RaftRocksdbOptions.initRocksdbGlobalConfig(rocksdbConfig);
        BusinessHandlerImpl.initRocksdb(rocksdbConfig, null);

        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(databasePath.toString());
        options.setRaftPath(databasePath.toString());

        HgStoreEngineOptions.FakePdOptions fakePdOptions =
                new HgStoreEngineOptions.FakePdOptions();
        fakePdOptions.setPartitionCount(2);
        fakePdOptions.setPeersList("127.0.0.1");
        fakePdOptions.setStoreList("127.0.0.1");
        options.setFakePdOptions(fakePdOptions);

        PdProvider pdProvider = new FakePdServiceProvider(fakePdOptions);
        PartitionManager partitionManager = new PartitionManager(pdProvider, options) {

            @Override
            public String getDbDataPath(int partitionId, String dbName) {
                return databasePath.resolve("data").toString();
            }

            @Override
            public boolean hasPartition(String graphName, int partitionId) {
                return partitionId == PARTITION_ID || partitionId == EMPTY_PARTITION_ID;
            }

            @Override
            public List<Integer> getLeaderPartitionIds(String graph) {
                return Collections.singletonList(PARTITION_ID);
            }
        };
        handler = new BusinessHandlerImpl(partitionManager);
        handler.createTable("setup", PARTITION_ID, VERTEX_TABLE);
    }

    @AfterClass
    public static void teardown() {
        if (handler != null) {
            handler.closeAll();
        }
        if (databasePath != null) {
            UnitTestBase.deleteDir(databasePath.toFile());
        }
    }

    @Test(timeout = 5000L)
    public void testFirstBatchOnEmptyPartitionCompletes() {
        String graph = "first-batch-empty-partition";
        byte[] value = "first-value".getBytes(StandardCharsets.UTF_8);

        Assert.assertFalse(handler.existsTable(graph, EMPTY_PARTITION_ID, VERTEX_TABLE));
        writeBatch(graph, EMPTY_PARTITION_ID, EMPTY_PARTITION_KEY_CODE,
                   OpType.OP_TYPE_PUT, value);
        Assert.assertArrayEquals(value, readByCode(graph, EMPTY_PARTITION_KEY_CODE));
    }

    @Test(timeout = 10000L)
    public void testTruncateWaitsForInFlightBatch() throws Exception {
        String graph = "in-flight-batch-graph";
        String nextGraph = "graph-after-truncate";
        byte[] pendingValue = "pending-value".getBytes(StandardCharsets.UTF_8);
        byte[] nextValue = "next-value".getBytes(StandardCharsets.UTF_8);
        BusinessHandler.TxBuilder builder = handler.txBuilder(graph, PARTITION_ID);
        BusinessHandler.Tx transaction = builder.build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch truncateStarted = new CountDownLatch(1);
        boolean committed = false;

        try {
            put(builder, VERTEX_TABLE, pendingValue);
            Future<?> truncate = executor.submit(() -> {
                truncateStarted.countDown();
                handler.truncate(graph, PARTITION_ID);
            });

            Assert.assertTrue(truncateStarted.await(1L, TimeUnit.SECONDS));
            try {
                truncate.get(500L, TimeUnit.MILLISECONDS);
                Assert.fail("truncate must wait for the in-flight batch");
            } catch (TimeoutException expected) {
                // Expected until the transaction releases its graph ID reservation.
            }

            transaction.commit();
            committed = true;
            truncate.get(5L, TimeUnit.SECONDS);

            writeBatch(nextGraph, OpType.OP_TYPE_PUT, nextValue);
            Assert.assertArrayEquals(nextValue, read(nextGraph));
        } finally {
            if (!committed) {
                transaction.rollback();
            }
            executor.shutdownNow();
        }
    }

    @Test
    public void testDataManagerRollsBackFailedBatch() {
        BusinessHandler mockHandler = Mockito.mock(BusinessHandler.class);
        BusinessHandler.TxBuilder mockBuilder = Mockito.mock(BusinessHandler.TxBuilder.class);
        BusinessHandler.Tx mockTransaction = Mockito.mock(BusinessHandler.Tx.class);
        BatchPutRequest request = batchPutRequest();
        BatchPutRequest.KV entry = request.getEntries().get(0);
        RuntimeException failure = new RuntimeException("injected put failure");
        Mockito.when(mockHandler.txBuilder(request.getGraphName(), request.getPartitionId()))
               .thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(mockTransaction);
        Mockito.doThrow(failure).when(mockBuilder)
               .put(entry.getCode(), entry.getTable(), entry.getKey(), entry.getValue());
        DataManagerImpl dataManager = new DataManagerImpl();
        dataManager.setBusinessHandler(mockHandler);

        assertWriteFails(failure, () -> dataManager.write(request));

        Mockito.verify(mockTransaction).rollback();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDefaultDataMoverRollsBackFailedBatch() {
        BusinessHandler mockHandler = Mockito.mock(BusinessHandler.class);
        BusinessHandler.TxBuilder mockBuilder = Mockito.mock(BusinessHandler.TxBuilder.class);
        BusinessHandler.Tx mockTransaction = Mockito.mock(BusinessHandler.Tx.class);
        BatchPutRequest request = batchPutRequest();
        BatchPutRequest.KV entry = request.getEntries().get(0);
        RuntimeException failure = new RuntimeException("injected put failure");
        Mockito.when(mockHandler.txBuilder(request.getGraphName(), request.getPartitionId()))
               .thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(mockTransaction);
        Mockito.doThrow(failure).when(mockBuilder)
               .put(entry.getCode(), entry.getTable(), entry.getKey(), entry.getValue());
        DefaultDataMover dataMover = new DefaultDataMover();
        dataMover.setBusinessHandler(mockHandler);

        assertWriteFails(failure, () -> dataMover.doWriteData(request));

        Mockito.verify(mockTransaction).rollback();
    }

    @Test
    public void testBatchPutKeepsGraphsIsolated() {
        String graph1 = "batch-put-graph-1";
        String graph2 = "batch-put-graph-2";
        byte[] value1 = "graph-1-value".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "graph-2-value".getBytes(StandardCharsets.UTF_8);

        writeBatch(graph1, OpType.OP_TYPE_PUT, value1);
        writeBatch(graph2, OpType.OP_TYPE_PUT, value2);

        Assert.assertArrayEquals(value1, read(graph1));
        Assert.assertArrayEquals(value2, read(graph2));

        handler.truncate(graph2, PARTITION_ID);
        Assert.assertNull(read(graph2));
        Assert.assertArrayEquals(value1, read(graph1));
    }

    @Test
    public void testBatchMergeKeepsGraphsIsolated() {
        String graph1 = "batch-merge-graph-1";
        String graph2 = "batch-merge-graph-2";

        writeBatch(graph1, OpType.OP_TYPE_MERGE, longToBytes(10L));
        writeBatch(graph2, OpType.OP_TYPE_MERGE, longToBytes(20L));

        Assert.assertEquals(10L, bytesToLong(read(graph1)));
        Assert.assertEquals(20L, bytesToLong(read(graph2)));
    }

    private static void writeBatch(String graph, OpType type, byte[] value) {
        writeBatch(graph, PARTITION_ID, type, value);
    }

    private static void writeBatch(String graph, int partitionId, OpType type, byte[] value) {
        writeBatch(graph, partitionId, KEY_CODE, type, value);
    }

    private static void writeBatch(String graph, int partitionId, int code, OpType type,
                                   byte[] value) {
        Key key = Key.newBuilder()
                     .setCode(code)
                     .setKey(ByteString.copyFrom(SHARED_KEY))
                     .build();
        BatchEntry entry = BatchEntry.newBuilder()
                                     .setOpType(type)
                                     .setTable(TABLES_MAP.get(VERTEX_TABLE))
                                     .setStartKey(key)
                                     .setValue(ByteString.copyFrom(value))
                                     .build();
        handler.doBatch(graph, partitionId, Collections.singletonList(entry));
    }

    private static void put(BusinessHandler.TxBuilder builder, String table, byte[] value) {
        builder.put(KEY_CODE, table, SHARED_KEY, value);
    }

    private static byte[] read(String graph) {
        return readByCode(graph, KEY_CODE);
    }

    private static byte[] readByCode(String graph, int code) {
        return handler.doGet(graph, code, VERTEX_TABLE, SHARED_KEY);
    }

    private static BatchPutRequest batchPutRequest() {
        BatchPutRequest request = new BatchPutRequest();
        request.setGraphName("failed-transfer-graph");
        request.setPartitionId(PARTITION_ID);
        request.getEntries()
               .add(BatchPutRequest.KV.of(VERTEX_TABLE, KEY_CODE, SHARED_KEY,
                                         "value".getBytes(StandardCharsets.UTF_8)));
        return request;
    }

    private static void assertWriteFails(RuntimeException expected, Runnable writer) {
        try {
            writer.run();
            Assert.fail("batch write must propagate its failure");
        } catch (RuntimeException actual) {
            Assert.assertSame(expected, actual);
        }
    }

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES)
                         .order(ByteOrder.LITTLE_ENDIAN)
                         .putLong(value)
                         .array();
    }

    private static long bytesToLong(byte[] value) {
        return ByteBuffer.wrap(value)
                         .order(ByteOrder.LITTLE_ENDIAN)
                         .getLong();
    }
}
