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

package org.apache.hugegraph.store.client;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq.Builder;
import org.junit.Assert;
import org.junit.Test;

public class NodeTxSessionProxyTest {

    @Test
    public void testNodeTkvDoesNotMutateSharedOwnerKeys() {
        HgOwnerKey start = HgOwnerKey.of(keyBytes(9), keyBytes(1));
        HgOwnerKey end = HgOwnerKey.of(keyBytes(9), keyBytes(5));
        start.setSerialNo(7);
        end.setSerialNo(8);

        NodeTkv first = new NodeTkv(HgNodePartition.of(1L, 0, 10, 20),
                                    "g+index", start, end);
        NodeTkv second = new NodeTkv(HgNodePartition.of(2L, 0, 30, 40),
                                     "g+index", start, end);

        Assert.assertEquals(0, start.getKeyCode());
        Assert.assertEquals(0, end.getKeyCode());
        Assert.assertEquals(10, first.getKey().getKeyCode());
        Assert.assertEquals(20, first.getEndKey().getKeyCode());
        Assert.assertEquals(30, second.getKey().getKeyCode());
        Assert.assertEquals(40, second.getEndKey().getKeyCode());
        Assert.assertEquals(7, first.getKey().getSerialNo());
        Assert.assertEquals(8, first.getEndKey().getSerialNo());
    }

    @Test
    public void testScanIteratorOrderedUsesPerPartitionBuildersLazily()
            throws Exception {
        HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        HgStoreNodePartitioner oldPartitioner = manager.getNodePartitioner();
        long firstNodeId = System.nanoTime();
        long secondNodeId = firstNodeId + 1L;
        RecordingPartitioner partitioner =
                new RecordingPartitioner(firstNodeId, secondNodeId);
        TestIterator firstIterator = new TestIterator(3, 4);
        TestIterator secondIterator = new TestIterator(1, 2);
        RecordingSession firstSession = new RecordingSession(firstIterator);
        RecordingSession secondSession = new RecordingSession(secondIterator);
        String graph = "graph-" + firstNodeId;
        manager.addNode(graph, new RecordingStoreNode(firstNodeId,
                                                      firstSession.proxy()));
        manager.addNode(graph, new RecordingStoreNode(secondNodeId,
                                                      secondSession.proxy()));
        manager.setNodePartitioner(partitioner);
        try {
            HgStoreSession proxy = new NodeTxSessionProxy(graph, manager);
            HgKvIterator<HgKvEntry> iterator = proxy.scanIteratorOrdered(
                    "table", HgOwnerKey.of(keyBytes(9), keyBytes(1)),
                    HgOwnerKey.of(keyBytes(9), keyBytes(5)), 5L, 123,
                    keyBytes(7));

            Assert.assertEquals(1, firstSession.builders.size());
            Assert.assertEquals(1, secondSession.builders.size());
            Assert.assertEquals(0, partitioner.ownerRangeCalls);
            Assert.assertEquals(1, partitioner.codeRangeCalls);
            Assert.assertEquals(0, partitioner.startCode);
            Assert.assertEquals(PartitionUtils.MAX_VALUE,
                                partitioner.endCode);
            Assert.assertEquals(0, firstSession.rangeScanCalls);
            Assert.assertEquals(0, secondSession.rangeScanCalls);
            Assert.assertEquals(0, firstIterator.nextCalls);
            Assert.assertEquals(0, secondIterator.nextCalls);
            assertOrderedRangeBuilder(firstSession.builders.get(0), 5L,
                                      123, 10, keyBytes(7));
            assertOrderedRangeBuilder(secondSession.builders.get(0), 5L,
                                      123, 30, keyBytes(7));

            Assert.assertEquals(1, key(iterator.next()));
            Assert.assertEquals(2, key(iterator.next()));
            Assert.assertEquals(3, key(iterator.next()));
            Assert.assertEquals(4, key(iterator.next()));
            Assert.assertFalse(iterator.hasNext());
        } finally {
            restoreNodePartitioner(manager, oldPartitioner);
        }
    }

    @Test
    public void testScanIteratorOrderedClosesOpenedIteratorsOnOpenFailure()
            throws Exception {
        HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        HgStoreNodePartitioner oldPartitioner = manager.getNodePartitioner();
        long firstNodeId = System.nanoTime();
        long secondNodeId = firstNodeId + 1L;
        RecordingPartitioner partitioner =
                new RecordingPartitioner(firstNodeId, secondNodeId);
        CountDownLatch firstOpened = new CountDownLatch(1);
        TestIterator firstIterator = new TestIterator(1);
        RecordingSession firstSession = new RecordingSession(
                firstIterator, firstOpened, null, false);
        RecordingSession secondSession = new RecordingSession(
                new TestIterator(2), null, firstOpened, true);
        String graph = "graph-open-failure-" + firstNodeId;
        manager.addNode(graph, new RecordingStoreNode(firstNodeId,
                                                      firstSession.proxy()));
        manager.addNode(graph, new RecordingStoreNode(secondNodeId,
                                                      secondSession.proxy()));
        manager.setNodePartitioner(partitioner);
        try {
            HgStoreSession proxy = new NodeTxSessionProxy(graph, manager);

            Assert.assertThrows(RuntimeException.class,
                                () -> proxy.scanIteratorOrdered(
                                        "table",
                                        HgOwnerKey.of(keyBytes(9), keyBytes(1)),
                                        HgOwnerKey.of(keyBytes(9), keyBytes(5)),
                                        5L, 123, keyBytes(7)));
            Assert.assertTrue(firstIterator.closed);
        } finally {
            restoreNodePartitioner(manager, oldPartitioner);
        }
    }

    private static void assertOrderedRangeBuilder(Builder builder, long limit,
                                                  int scanType, int code,
                                                  byte[] query) {
        Assert.assertEquals(ScanMethod.RANGE, builder.getMethod());
        Assert.assertEquals("table", builder.getTable());
        Assert.assertEquals(limit, builder.getLimit());
        Assert.assertEquals(scanType, builder.getScanType());
        Assert.assertEquals(code, builder.getCode());
        Assert.assertArrayEquals(keyBytes(1), builder.getStart().toByteArray());
        Assert.assertArrayEquals(keyBytes(5), builder.getEnd().toByteArray());
        Assert.assertArrayEquals(query, builder.getQuery().toByteArray());
    }

    private static void restoreNodePartitioner(HgStoreNodeManager manager,
                                               HgStoreNodePartitioner old)
            throws Exception {
        Field field = HgStoreNodeManager.class.getDeclaredField(
                "nodePartitioner");
        field.setAccessible(true);
        field.set(manager, old);
    }

    private static int key(HgKvEntry entry) {
        return entry.key()[0] & 0xff;
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements HgKvIterator<HgKvEntry> {

        private final List<Integer> keys;
        private int offset;
        private int nextCalls;
        private HgKvEntry current;
        private boolean closed;

        private TestIterator(Integer... keys) {
            this.keys = Arrays.asList(keys);
            this.offset = 0;
            this.nextCalls = 0;
            this.current = null;
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return this.offset < this.keys.size();
        }

        @Override
        public HgKvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.nextCalls++;
            this.current = new TestEntry(keyBytes(this.keys.get(this.offset++)));
            return this.current;
        }

        @Override
        public byte[] key() {
            return this.current == null ? null : this.current.key();
        }

        @Override
        public byte[] value() {
            return this.current == null ? null : this.current.value();
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }

    private static final class TestEntry implements HgKvEntry {

        private final byte[] key;

        private TestEntry(byte[] key) {
            this.key = key;
        }

        @Override
        public byte[] key() {
            return this.key;
        }

        @Override
        public byte[] value() {
            return this.key;
        }
    }

    private static final class RecordingPartitioner
            implements HgStoreNodePartitioner {

        private final long firstNodeId;
        private final long secondNodeId;
        private int ownerRangeCalls;
        private int codeRangeCalls;
        private int startCode;
        private int endCode;

        private RecordingPartitioner(long firstNodeId, long secondNodeId) {
            this.firstNodeId = firstNodeId;
            this.secondNodeId = secondNodeId;
            this.ownerRangeCalls = 0;
            this.codeRangeCalls = 0;
            this.startCode = -1;
            this.endCode = -1;
        }

        @Override
        public int partition(HgNodePartitionerBuilder builder,
                             String graphName, byte[] startKey,
                             byte[] endKey) {
            this.ownerRangeCalls++;
            return this.setPartitions(builder);
        }

        @Override
        public int partition(HgNodePartitionerBuilder builder,
                             String graphName, int startCode,
                             int endCode) {
            this.codeRangeCalls++;
            this.startCode = startCode;
            this.endCode = endCode;
            return this.setPartitions(builder);
        }

        private int setPartitions(HgNodePartitionerBuilder builder) {
            Set<HgNodePartition> partitions = new LinkedHashSet<>();
            partitions.add(HgNodePartition.of(this.firstNodeId, 10, 10, 20));
            partitions.add(HgNodePartition.of(this.secondNodeId, 30, 30, 40));
            builder.setPartitions(partitions);
            return 0;
        }
    }

    private static final class RecordingStoreNode implements HgStoreNode {

        private final Long nodeId;
        private final HgStoreSession session;

        private RecordingStoreNode(Long nodeId, HgStoreSession session) {
            this.nodeId = nodeId;
            this.session = session;
        }

        @Override
        public Long getNodeId() {
            return this.nodeId;
        }

        @Override
        public String getAddress() {
            return "127.0.0.1:" + this.nodeId;
        }

        @Override
        public HgStoreSession openSession(String graphName) {
            return this.session;
        }
    }

    private static final class RecordingSession implements InvocationHandler {

        private final List<Builder> builders;
        private final TestIterator iterator;
        private final CountDownLatch opened;
        private final CountDownLatch waitBeforeFailure;
        private final boolean failOnBuilder;
        private int rangeScanCalls;

        private RecordingSession(TestIterator iterator) {
            this(iterator, null, null, false);
        }

        private RecordingSession(TestIterator iterator,
                                 CountDownLatch opened,
                                 CountDownLatch waitBeforeFailure,
                                 boolean failOnBuilder) {
            this.builders = Collections.synchronizedList(new ArrayList<>());
            this.iterator = iterator;
            this.opened = opened;
            this.waitBeforeFailure = waitBeforeFailure;
            this.failOnBuilder = failOnBuilder;
            this.rangeScanCalls = 0;
        }

        private HgStoreSession proxy() {
            return (HgStoreSession) Proxy.newProxyInstance(
                    HgStoreSession.class.getClassLoader(),
                    new Class[]{HgStoreSession.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if ("scanIterator".equals(method.getName())) {
                if (args != null && args.length == 1 &&
                    args[0] instanceof Builder) {
                    if (this.waitBeforeFailure != null) {
                        try {
                            if (!this.waitBeforeFailure.await(5L,
                                                              TimeUnit.SECONDS)) {
                                throw new IllegalStateException(
                                        "Timed out waiting for first iterator");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                    }
                    if (this.failOnBuilder) {
                        throw new IllegalStateException("injected failure");
                    }
                    this.builders.add(((Builder) args[0]).clone());
                    if (this.opened != null) {
                        this.opened.countDown();
                    }
                    return this.iterator;
                }
                this.rangeScanCalls++;
                return this.iterator;
            }
            if ("isTx".equals(method.getName())) {
                return false;
            }
            if ("toString".equals(method.getName())) {
                return "RecordingSession";
            }
            throw new UnsupportedOperationException(method.toString());
        }
    }
}
