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

package org.apache.hugegraph.pd.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.StoreStatusListener;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.PartitionMeta;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.Node;

public class StorePlacementSubmissionTest {

    @Test
    public void testTombstoneNotificationDoesNotInvertPlacementLocks() throws Exception {
        StoreNodeService stores = Mockito.mock(StoreNodeService.class,
                                               Mockito.CALLS_REAL_METHODS);
        StoreInfoMeta meta = Mockito.mock(StoreInfoMeta.class);
        PartitionService partitions = Mockito.mock(PartitionService.class,
                                                   Mockito.CALLS_REAL_METHODS);
        PartitionMeta partitionMeta = Mockito.mock(PartitionMeta.class);
        setField(PartitionService.class, partitions, "partitionMeta", partitionMeta);
        setField(PartitionService.class, partitions, "storeService", stores);
        Metapb.Store source = store(1L, Metapb.StoreState.Up);
        Metapb.Store destination = store(2L, Metapb.StoreState.Up);
        Metapb.Store retiring = store(3L, Metapb.StoreState.Exiting);
        Map<Long, Metapb.Store> current = new HashMap<>();
        current.put(source.getId(), source);
        current.put(destination.getId(), destination);
        current.put(retiring.getId(), retiring);
        Mockito.when(meta.getActiveStores()).thenReturn(List.of(source, destination));
        Mockito.when(meta.getStores(null)).thenReturn(List.of(source, destination, retiring));
        Mockito.when(meta.getShardGroups()).thenReturn(List.of());
        Mockito.when(meta.getStore(Mockito.anyLong()))
               .thenAnswer(call -> current.get((Long) call.getArgument(0)));
        Mockito.doAnswer(call -> {
            Metapb.Store store = call.getArgument(0);
            current.put(store.getId(), store);
            return null;
        }).when(meta).updateStore(Mockito.any());
        setField(StoreNodeService.class, stores, "storeInfoMeta", meta);
        setField(StoreNodeService.class, stores, "pdConfig", new PDConfig());

        CountDownLatch partitionHeld = new CountDownLatch(1);
        CountDownLatch callbackEntered = new CountDownLatch(1);
        List<StoreStatusListener> listeners = Collections.synchronizedList(new ArrayList<>());
        setField(StoreNodeService.class, stores, "statusListeners", listeners);
        partitions.init();
        Metapb.Partition partition = Metapb.Partition.newBuilder()
                                                   .setId(0).setGraphName("graph").build();
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("graph").build();
        Mockito.when(partitionMeta.getGraph("graph")).thenReturn(graph);
        Mockito.doNothing().when(partitions).reallocPartitionShards(graph, partition);
        // Keep the installed production Tombstone listener, storeOffline() and the
        // synchronized shardOffline() real. Control only its metadata-query boundary.
        Mockito.doAnswer(call -> {
            callbackEntered.countDown();
            // Fail before blocking if the old lock order reappears, so the test always
            // releases both threads instead of leaving deadlocked monitor owners behind.
            Assert.assertFalse(Thread.holdsLock(stores));
            Assert.assertFalse(Thread.holdsLock(listeners));
            return List.of(partition);
        }).when(partitions).getPartitionByStore(Mockito.any());

        TaskScheduleService tasks = Mockito.mock(TaskScheduleService.class,
                                                 Mockito.CALLS_REAL_METHODS);
        setField(TaskScheduleService.class, tasks, "storeService", stores);
        setField(TaskScheduleService.class, tasks, "partitionService", partitions);
        RaftEngine raft = RaftEngine.getInstance();
        Field nodeField = RaftEngine.class.getDeclaredField("raftNode");
        nodeField.setAccessible(true);
        Object previous = nodeField.get(raft);
        Node node = Mockito.mock(Node.class);
        Mockito.when(node.isLeader(true)).thenReturn(true);
        nodeField.set(raft, node);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> placement = workers.submit(() -> {
                synchronized (partitions) {
                    partitionHeld.countDown();
                    Assert.assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
                    // The old retirement callback is waiting for this PartitionService
                    // monitor. A concurrent management update must not revive its ID.
                    PDException error = Assert.assertThrows(PDException.class, () ->
                            stores.updateStore(retiring.toBuilder()
                                                       .setState(Metapb.StoreState.Up).build()));
                    Assert.assertEquals(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                        error.getErrorCode());
                    Map<Integer, KVPair<Long, Long>> plan = new HashMap<>();
                    plan.put(0, new KVPair<>(source.getId(), destination.getId()));
                    Assert.assertEquals(1, tasks.movePartitions(plan).size());
                }
                return null;
            });
            Assert.assertTrue(partitionHeld.await(5, TimeUnit.SECONDS));
            Future<?> retirement = workers.submit(() -> stores.updateStore(
                    retiring.toBuilder().setState(Metapb.StoreState.Tombstone).build()));
            retirement.get(5, TimeUnit.SECONDS);
            placement.get(5, TimeUnit.SECONDS);
            Mockito.verify(partitions).shardOffline(partition, retiring.getId());
            Mockito.verify(partitions).movePartitionsShard(0, source.getId(), destination.getId());
            Assert.assertEquals(Metapb.StoreState.Tombstone, current.get(retiring.getId()).getState());
        } finally {
            callbackEntered.countDown();
            workers.shutdownNow();
            try {
                Assert.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                nodeField.set(raft, previous);
            }
        }
    }

    @Test
    public void testMovePlanRevalidatesDestinationBeforeSubmission() throws Exception {
        StoreNodeService stores = Mockito.mock(StoreNodeService.class,
                                               Mockito.CALLS_REAL_METHODS);
        StoreInfoMeta meta = Mockito.mock(StoreInfoMeta.class);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        Map<Long, Metapb.Store> current = new HashMap<>();
        Metapb.Store source = store(1L, Metapb.StoreState.Up);
        Metapb.Store destinationLease = store(2L, Metapb.StoreState.Up);
        current.put(source.getId(), source);
        current.put(destinationLease.getId(),
                    destinationLease.toBuilder().setState(Metapb.StoreState.Exiting).build());
        Mockito.when(meta.getActiveStores()).thenReturn(List.of(source, destinationLease));
        Mockito.when(meta.getStore(Mockito.anyLong())).thenAnswer(call ->
                current.get((Long) call.getArgument(0)));
        setField(StoreNodeService.class, stores, "storeInfoMeta", meta);

        TaskScheduleService tasks = Mockito.mock(TaskScheduleService.class,
                                                 Mockito.CALLS_REAL_METHODS);
        setField(TaskScheduleService.class, tasks, "storeService", stores);
        setField(TaskScheduleService.class, tasks, "partitionService", partitions);

        RaftEngine raft = RaftEngine.getInstance();
        Field nodeField = RaftEngine.class.getDeclaredField("raftNode");
        nodeField.setAccessible(true);
        Object previous = nodeField.get(raft);
        Node node = Mockito.mock(Node.class);
        Mockito.when(node.isLeader(true)).thenReturn(true);
        nodeField.set(raft, node);
        try {
            Map<Integer, KVPair<Long, Long>> stalePlan = new HashMap<>();
            stalePlan.put(0, new KVPair<>(source.getId(), destinationLease.getId()));
            Assert.assertTrue(tasks.movePartitions(stalePlan).isEmpty());
            Mockito.verify(partitions, Mockito.never())
                   .movePartitionsShard(Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());

            current.put(destinationLease.getId(), destinationLease);
            Map<Integer, KVPair<Long, Long>> validPlan = new HashMap<>();
            validPlan.put(0, new KVPair<>(source.getId(), destinationLease.getId()));
            Assert.assertEquals(1, tasks.movePartitions(validPlan).size());
            Mockito.verify(partitions).movePartitionsShard(0, source.getId(),
                                                           destinationLease.getId());
        } finally {
            nodeField.set(raft, previous);
        }
    }

    private static Metapb.Store store(long id, Metapb.StoreState state) {
        return Metapb.Store.newBuilder().setId(id).setState(state).build();
    }

    private static void setField(Class<?> owner, Object target, String name, Object value)
            throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
