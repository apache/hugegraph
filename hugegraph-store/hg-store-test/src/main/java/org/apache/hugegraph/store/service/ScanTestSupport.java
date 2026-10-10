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

package org.apache.hugegraph.store.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.ScanMethod;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.grpc.HgStoreWrapperEx;
import org.mockito.ArgumentCaptor;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

abstract class ScanTestSupport {

    static HgStoreStreamImpl scanService(ThreadPoolExecutor executor,
                                                 HgStoreWrapperEx wrapper) throws Exception {
        HgStoreStreamImpl service = new HgStoreStreamImpl();
        AppConfig config = mock(AppConfig.class);
        when(config.getServerWaitTime()).thenReturn(5);
        for (String name : new String[]{"executor", "wrapper", "appConfig"}) {
            Field field = HgStoreStreamImpl.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(service, name.equals("executor") ? executor : name.equals("wrapper") ? wrapper : config);
        }
        return service;
    }

    static Map<?, ?> scanRegistry(HgStoreStreamImpl service) throws Exception {
        Field scans = HgStoreStreamImpl.class.getDeclaredField("scans");
        scans.setAccessible(true);
        return (Map<?, ?>) scans.get(service);
    }

    static void awaitTimedWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        Thread.State observed = thread.getState();
        while (observed != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.yield();
            observed = thread.getState();
        }
        assertEquals(Thread.State.TIMED_WAITING, observed);
    }

    static ScanStreamBatchReq batchRequest() {
        return ScanStreamBatchReq.newBuilder().setHeader(Header.newBuilder().setGraph("g"))
                                 .setQueryRequest(ScanQueryRequest.newBuilder().setMethod(ScanMethod.ALL)
                                                                 .setTable("t").setLimit(10)
                                                                 .setPerKeyMax(Long.MAX_VALUE)
                                                                 .setPageSize(1)).build();
    }

    static void assertCancelled(StreamObserver<KvPageRes> output) {
        ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
        verify(output).onError(failure.capture());
        assertEquals(Status.Code.CANCELLED, Status.fromThrowable(failure.getValue()).getCode());
    }

    static void assertUnavailable(Runnable action) {
        try {
            action.run();
            fail("Scan admission must be closed");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
        }
    }
}
