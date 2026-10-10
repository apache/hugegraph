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

package org.apache.hugegraph.store.node.grpc.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;

abstract class AggregativeQueryTestSupport {

    static void awaitShutdownWait(Thread closer) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (System.nanoTime() < deadline) {
            if (closer.getState() == Thread.State.TIMED_WAITING &&
                Arrays.stream(closer.getStackTrace()).anyMatch(frame ->
                        frame.getClassName().equals(AggregativeQueryService.class.getName()) &&
                        frame.getMethodName().equals("shutdownQueries"))) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        fail("shutdown must enter the aggregate-query cleanup wait");
    }

    static void assertError(ResponseRecorder response, String queryId) {
        assertEquals(1, response.responses.size());
        QueryResponse error = response.responses.get(0);
        assertEquals(queryId, error.getQueryId());
        assertFalse(error.getIsOk());
        assertFalse(error.getIsFinished());
        assertFalse(error.getMessage().isEmpty());
    }

    static ThreadPoolExecutor pool(int size) {
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(size);
    }

    static AggregativeQueryService service(ThreadPoolExecutor pool, ScanIterator iterator,
                                                   QueryPlan plan, long timeout) {
        return new AggregativeQueryService(pool, timeout, 10) {
            @Override
            AggregativeQueryObserver newObserver(StreamObserver<QueryResponse> sender) {
                return fixture(super.newObserver(sender), iterator, plan, null);
            }
        };
    }

    static AggregativeQueryObserver fixture(AggregativeQueryObserver observer,
                                                     ScanIterator iterator,
                                                     QueryPlan plan, CountDownLatch parentReturned) {
        AggregativeQueryObserver managed = spy(observer);
        doReturn(iterator).when(managed).getIterator(org.mockito.ArgumentMatchers.any());
        doReturn(plan).when(managed).buildPlan(org.mockito.ArgumentMatchers.any());
        if (parentReturned != null) {
            doAnswer(invocation -> {
                try {
                    invocation.callRealMethod();
                    return null;
                } finally {
                    parentReturned.countDown();
                }
            }).when(managed).sendData();
        }
        return managed;
    }

    @SuppressWarnings("unchecked")
    static StreamObserver<QueryResponse> sender() {
        return mock(StreamObserver.class);
    }

    static FutureTask<Void> close(AggregativeQueryService service) {
        return new FutureTask<>(() -> {
            service.shutdownQueries();
            return null;
        });
    }

    static Thread start(FutureTask<?> task) {
        Thread thread = new Thread(task, "test-aggregate-query-close");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class ResponseRecorder implements StreamObserver<QueryResponse> {

        final List<QueryResponse> responses = new CopyOnWriteArrayList<>();
        final AtomicInteger completed = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final CountDownLatch received = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);

        @Override
        public void onNext(QueryResponse response) {
            this.responses.add(response);
            this.received.countDown();
        }

        @Override
        public void onError(Throwable error) {
            this.errors.incrementAndGet();
            this.finished.countDown();
        }

        @Override
        public void onCompleted() {
            this.completed.incrementAndGet();
            this.finished.countDown();
        }
    }
}
