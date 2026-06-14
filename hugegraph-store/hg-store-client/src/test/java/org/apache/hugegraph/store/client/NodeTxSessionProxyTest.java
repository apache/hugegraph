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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.junit.Assert;
import org.junit.Test;

public class NodeTxSessionProxyTest {

    @Test
    public void testNodeTkvDoesNotMutateSharedOwnerKeys() {
        HgOwnerKey start = HgOwnerKey.of(keyBytes(9), keyBytes(1));
        HgOwnerKey end = HgOwnerKey.of(keyBytes(9), keyBytes(5));

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
    }

    @Test
    public void testMergeRangeScanIteratorsKeepsLegacyTopWorkOrder() {
        HgKvIterator<HgKvEntry> iterator = NodeTxSessionProxy.mergeRangeScanIterators(
                Arrays.asList(new TestIterator(3, 4), new TestIterator(1, 2)), 0L);

        Assert.assertEquals(3, key(iterator.next()));
        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertEquals(4, key(iterator.next()));
        Assert.assertEquals(2, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testMergeOrderedRangeScanIteratorsSortsByKey() {
        HgKvIterator<HgKvEntry> iterator =
                NodeTxSessionProxy.mergeOrderedRangeScanIterators(
                        Arrays.asList(new TestIterator(3, 4),
                                      new TestIterator(1, 2)), 0L);

        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertEquals(2, key(iterator.next()));
        Assert.assertEquals(3, key(iterator.next()));
        Assert.assertEquals(4, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testPrefetchOrderedRangeScanIteratorsDoesNotConsumeEntries() {
        TestIterator first = new TestIterator(3, 4);
        TestIterator second = new TestIterator(1, 2);
        NodeTxSessionProxy.prefetchOrderedRangeScanIterators(
                Arrays.asList(first, second));

        Assert.assertTrue(first.hasNextCalls > 0);
        Assert.assertTrue(second.hasNextCalls > 0);
        Assert.assertEquals(0, first.nextCalls);
        Assert.assertEquals(0, second.nextCalls);

        HgKvIterator<HgKvEntry> iterator =
                NodeTxSessionProxy.mergeOrderedRangeScanIterators(
                        Arrays.asList(first, second), 0L);
        Assert.assertEquals(1, key(iterator.next()));
        Assert.assertEquals(2, key(iterator.next()));
        Assert.assertEquals(3, key(iterator.next()));
        Assert.assertEquals(4, key(iterator.next()));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testOrderedRangeNodeTkvListKeepsLegacyOwnerPartitioning()
            throws Exception {
        HgStoreNodeManager manager = HgStoreNodeManager.getInstance();
        HgStoreNodePartitioner oldPartitioner = manager.getNodePartitioner();
        RecordingPartitioner partitioner = new RecordingPartitioner();
        manager.setNodePartitioner(partitioner);
        try {
            NodeTxSessionProxy proxy = new NodeTxSessionProxy("graph", manager);
            List<NodeTkv> nodeTkvs = toOrderedRangeNodeTkvList(
                    proxy, HgOwnerKey.of(keyBytes(9), keyBytes(1)),
                    HgOwnerKey.of(keyBytes(9), keyBytes(5)));

            Assert.assertEquals(1, partitioner.byteRangeCalls);
            Assert.assertEquals(0, partitioner.codeRangeCalls);
            Assert.assertArrayEquals(keyBytes(9), partitioner.lastStartKey);
            Assert.assertArrayEquals(keyBytes(9), partitioner.lastEndKey);
            Assert.assertEquals(2, nodeTkvs.size());
            Assert.assertEquals(10, nodeTkvs.get(0).getKey().getKeyCode());
            Assert.assertEquals(20, nodeTkvs.get(0).getEndKey().getKeyCode());
            Assert.assertEquals(30, nodeTkvs.get(1).getKey().getKeyCode());
            Assert.assertEquals(40, nodeTkvs.get(1).getEndKey().getKeyCode());
        } finally {
            restoreNodePartitioner(manager, oldPartitioner);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<NodeTkv> toOrderedRangeNodeTkvList(
            NodeTxSessionProxy proxy, HgOwnerKey startKey,
            HgOwnerKey endKey) throws Exception {
        Method method = NodeTxSessionProxy.class.getDeclaredMethod(
                "toOrderedRangeNodeTkvList", String.class, HgOwnerKey.class,
                HgOwnerKey.class);
        method.setAccessible(true);
        return (List<NodeTkv>) method.invoke(proxy, "table", startKey, endKey);
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
        return entry.key()[0] & 0xFF;
    }

    private static byte[] keyBytes(int key) {
        return new byte[]{(byte) key};
    }

    private static final class TestIterator implements HgKvIterator<HgKvEntry> {

        private final List<Integer> keys;
        private int offset;
        private HgKvEntry current;
        private int hasNextCalls;
        private int nextCalls;

        private TestIterator(Integer... keys) {
            this.keys = Arrays.asList(keys);
            this.offset = 0;
            this.current = null;
            this.hasNextCalls = 0;
            this.nextCalls = 0;
        }

        @Override
        public boolean hasNext() {
            this.hasNextCalls++;
            return this.offset < this.keys.size();
        }

        @Override
        public HgKvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.nextCalls++;
            int key = this.keys.get(this.offset++);
            this.current = new TestEntry(keyBytes(key));
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
        public byte[] position() {
            return this.key();
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

        private int byteRangeCalls;
        private int codeRangeCalls;
        private byte[] lastStartKey;
        private byte[] lastEndKey;

        @Override
        public int partition(HgNodePartitionerBuilder builder,
                             String graphName, byte[] startKey,
                             byte[] endKey) {
            this.byteRangeCalls++;
            this.lastStartKey = startKey;
            this.lastEndKey = endKey;
            builder.setPartitions(partitions());
            return 0;
        }

        @Override
        public int partition(HgNodePartitionerBuilder builder,
                             String graphName, int startCode,
                             int endCode) {
            this.codeRangeCalls++;
            builder.setPartitions(partitions());
            return 0;
        }

        private static Set<HgNodePartition> partitions() {
            Set<HgNodePartition> partitions = new LinkedHashSet<>();
            partitions.add(HgNodePartition.of(1L, 10, 10, 20));
            partitions.add(HgNodePartition.of(2L, 30, 30, 40));
            return partitions;
        }
    }
}
