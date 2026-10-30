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

package org.apache.hugegraph.pd.service;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.pd.LogService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

public class PDServiceDrainSubmissionTest {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSetStorePassesAcceptedDrainGroupsToStateTransition() throws Exception {
        PDService service = Mockito.spy(new PDService());
        Mockito.doReturn(true).when(service).isLeader();

        StoreNodeService stores = Mockito.mock(StoreNodeService.class);
        TaskScheduleService tasks = Mockito.mock(TaskScheduleService.class);
        LogService logs = Mockito.mock(LogService.class);
        PDConfig config = new PDConfig();
        config.setMinStoreCount(1);
        setField(service, "storeNodeService", stores);
        setField(service, "taskService", tasks);
        setField(service, "logService", logs);
        setField(service, "pdConfig", config);

        long sourceId = 4L;
        Metapb.Store source = store(sourceId);
        Mockito.when(stores.getStore(sourceId)).thenReturn(source);
        Mockito.when(stores.getPlacementStores())
               .thenReturn(List.of(store(1L), store(2L), store(3L), source));
        Mockito.when(stores.checkStoreCanOffline(Mockito.any())).thenReturn(true);

        Map<Integer, KVPair<Long, Long>> plan = new HashMap<>();
        plan.put(7, new KVPair<>(sourceId, 1L));
        Map<String, Object> result = new HashMap<>();
        result.put("flag", true);
        result.put("current_store_is_online", true);
        result.put("movedPartitions", plan);
        Mockito.when(tasks.canAllPartitionsMovedOut(source)).thenReturn(result);
        Mockito.when(tasks.movePartitions(plan)).thenReturn(plan);

        Metapb.Store exiting = source.toBuilder()
                                      .setState(Metapb.StoreState.Exiting).build();
        Mockito.when(stores.updateStore(Mockito.any())).thenReturn(exiting);
        Mockito.when(stores.updateStore(Mockito.any(), Mockito.anySet()))
               .thenReturn(exiting);
        StreamObserver<Pdpb.SetStoreResponse> observer = Mockito.mock(StreamObserver.class);
        Pdpb.SetStoreRequest request = Pdpb.SetStoreRequest.newBuilder()
                                                          .setStore(source.toBuilder()
                                                                          .setState(
                                                                                  Metapb.StoreState.Tombstone))
                                                          .build();

        service.setStore(request, observer);

        ArgumentCaptor<Metapb.Store> storeCaptor =
                ArgumentCaptor.forClass(Metapb.Store.class);
        ArgumentCaptor<Set<Integer>> groupsCaptor =
                ArgumentCaptor.forClass((Class) Set.class);
        Mockito.verify(stores).updateStore(storeCaptor.capture(), groupsCaptor.capture());
        Assert.assertEquals(Metapb.StoreState.Exiting, storeCaptor.getValue().getState());
        Assert.assertEquals(Set.of(7), groupsCaptor.getValue());
        Mockito.verify(tasks).movePartitions(plan);
        Mockito.verify(observer).onNext(Mockito.argThat(response ->
                response.getHeader().getError().getType() == Pdpb.ErrorType.OK));
        Mockito.verify(observer).onCompleted();
    }

    private static Metapb.Store store(long id) {
        return Metapb.Store.newBuilder()
                           .setId(id)
                           .setState(Metapb.StoreState.Up)
                           .setStats(Metapb.StoreStats.newBuilder().setStoreId(id))
                           .build();
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = PDService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
