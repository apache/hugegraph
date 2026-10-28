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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

public class StoreShutdownReportTest {

    private StoreNodeService stores;
    private StoreInfoMeta metadata;
    private PDService service;
    private Metapb.Store original;

    @Before
    public void setUp() throws Exception {
        stores = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        metadata = Mockito.mock(StoreInfoMeta.class);
        original = Metapb.Store.newBuilder().setId(7).setState(Metapb.StoreState.Up)
                .setRaftAddress("127.0.0.1:8510")
                .addLabels(Metapb.StoreLabel.newBuilder().setKey("zone").setValue("a")).build();
        Mockito.when(metadata.getStore(7L)).thenReturn(original);
        Whitebox.setInternalState(stores, "storeInfoMeta", metadata);
        Whitebox.setInternalState(stores, "statusListeners", List.of());
        Mockito.doReturn(Metapb.ClusterStats.newBuilder()
                .setState(Metapb.ClusterState.Cluster_Not_Ready).build()).when(stores).getClusterStats();
        service = new PDService() {
            @Override
            public boolean isLeader() {
                return true;
            }
        };
        Whitebox.setInternalState(service, "storeNodeService", stores);
    }

    @Test
    public void testShutdownReportBypassesReadinessAndPreservesMetadataAndLease() throws Exception {
        AtomicReference<Pdpb.SetStoreResponse> response = new AtomicReference<>();
        service.reportStoreShutdown(request(), observer(response));
        Assert.assertEquals(Pdpb.ErrorType.OK, response.get().getHeader().getError().getType());
        Metapb.Store expected = original.toBuilder().setState(Metapb.StoreState.Offline).build();
        Assert.assertEquals(expected, response.get().getStore());
        Mockito.verify(metadata).updateStore(expected);
        Mockito.verify(metadata, Mockito.never()).removeActiveStore(Mockito.any());
        Mockito.verify(metadata, Mockito.never()).keepStoreAlive(Mockito.any());
    }

    @Test
    public void testAdministrativeOfflineStillRequiresHealthyCluster() throws Exception {
        AtomicReference<Pdpb.SetStoreResponse> response = new AtomicReference<>();
        service.setStore(request(), observer(response));
        Assert.assertNotEquals(Pdpb.ErrorType.OK, response.get().getHeader().getError().getType());
        Mockito.verify(metadata, Mockito.never()).updateStore(Mockito.any());
    }

    @Test
    public void testRegistrationReturnsAuthoritativeState() throws Exception {
        Metapb.Store offline = original.toBuilder().setState(Metapb.StoreState.Offline).build();
        Mockito.doReturn(offline).when(stores).register(Mockito.any());
        AtomicReference<Pdpb.RegisterStoreResponse> response = new AtomicReference<>();
        service.registerStore(Pdpb.RegisterStoreRequest.newBuilder().setStore(offline).build(), observer(response));
        Assert.assertEquals(7L, response.get().getStoreId());
        Assert.assertEquals(offline, response.get().getStore());
    }

    private Pdpb.SetStoreRequest request() {
        return Pdpb.SetStoreRequest.newBuilder().setStore(Metapb.Store.newBuilder()
                .setId(7).setState(Metapb.StoreState.Offline).setRaftAddress("ignored")).build();
    }

    private static <T> StreamObserver<T> observer(AtomicReference<T> response) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable failure) {
                throw new AssertionError(failure);
            }

            @Override
            public void onCompleted() {
            }
        };
    }
}
