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

package org.apache.hugegraph.store.node.grpc;

import static org.apache.hugegraph.store.node.grpc.ScanUtil.getIterator;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.util.HgAssert;
import org.apache.hugegraph.store.node.util.HgChannel;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.HgStoreNodeUtil;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/02/17
 *
 * @version 3.6.0
 */
@Slf4j
public class ScanStreamResponse implements StreamObserver<ScanStreamReq> {

    private static final String msg =
            "to wait for client taking data exceeded max time: [{}] seconds,stop scanning.";
    private final StreamObserver<KvPageRes> responseObserver;
    private final HgStoreWrapperEx wrapper;
    private final AtomicBoolean finishFlag = new AtomicBoolean();
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final AtomicBoolean isStop = new AtomicBoolean(false);
    private final AppConfig config;
    private final int waitTime;
    private final HgChannel<KvPageRes.Builder> channel;
    private ScanIterator iterator;
    private final Object cancellationLock = new Object();
    private final Object responseLock = new Object();
    private Thread worker;
    private final ScanLifecycle lifecycle;

    private void cancel() {
        this.isStop.set(true);
        this.channel.close();
        Thread current = Thread.currentThread();
        synchronized (this.cancellationLock) {
            if (this.worker != null && this.worker != current) {
                this.worker.interrupt();
            }
        }
    }
    private long limit = 0;
    private int times = 0;
    private long pageSize = 0;
    private int total = 0;
    private int responseVersion = 0;
    private String graph;
    private String table;

    ScanStreamResponse(StreamObserver<KvPageRes> responseObserver,
                       HgStoreWrapperEx wrapper,
                       ThreadPoolExecutor executor, AppConfig appConfig) {
        this(responseObserver, wrapper, executor, appConfig, new ScanLifecycle());
    }

    ScanStreamResponse(StreamObserver<KvPageRes> responseObserver,
                       HgStoreWrapperEx wrapper, ThreadPoolExecutor executor,
                       AppConfig appConfig, ScanLifecycle lifecycle) {
        this.lifecycle = lifecycle;
        this.responseObserver = lifecycle.response(responseObserver);
        this.wrapper = wrapper;
        this.executor = executor;
        this.config = appConfig;
        this.waitTime = this.config.getServerWaitTime();
        this.channel = HgChannel.of(waitTime);
    }

    public static ScanStreamResponse of(StreamObserver<KvPageRes> responseObserver,
                                        HgStoreWrapperEx wrapper,
                                        ThreadPoolExecutor executor, AppConfig appConfig) {
        HgAssert.isArgumentNotNull(responseObserver, "responseObserver");
        HgAssert.isArgumentNotNull(wrapper, "wrapper");
        HgAssert.isArgumentNotNull(executor, "executor");
        return new ScanStreamResponse(responseObserver, wrapper, executor, appConfig);
    }

    @Override
    public void onNext(ScanStreamReq request) {
        if (this.isStop.get()) {
            return;
        }
        try {
            this.responseVersion = Math.max(
                    this.responseVersion, ScanUtil.responseVersion(request));
            if (request.getCloseFlag() == 1) {
                close();
            } else {
                next(request);
            }
        } catch (Exception e) {
            this.failServer(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        this.cancelServer(false);
        log.warn("onError from client [ graph: {} , table: {}]; Reason: {}]", graph, table,
                 t.getMessage());
    }

    @Override
    public void onCompleted() {
        this.cancelServer(false);
    }

    private void initIterator(ScanStreamReq request) {
        try {
            if (this.isStarted.getAndSet(true)) {
                return;
            }
            this.graph = request.getHeader().getGraph();
            this.table = request.getTable();
            this.limit = request.getLimit();
            this.pageSize = request.getPageSize();
            if (this.pageSize <= 0) {
                log.warn(
                        "As page-Size is less than or equals 0, no data will be send to the " +
                        "client.");
            }
            /*** Start scanning loop ***/
            Runnable scanning = () ->
            {
                // log.debug("Start scanning, graph = {}, table= {}, limit = " +
                //          "{}, page size = {}", this.graph, this.table, this.limit,
                //          this.pageSize);
                KvPageRes.Builder dataBuilder = KvPageRes.newBuilder();
                Kv.Builder kvBuilder = Kv.newBuilder();
                int pageCount = 0;
                try {
                    synchronized (this.cancellationLock) {
                        if (this.isStop.get()) {
                            return;
                        }
                        this.worker = Thread.currentThread();
                    }
                    this.iterator = getIterator(request, this.wrapper, this.lifecycle::failedCleanup);
                    while (!this.isStop.get() && !Thread.currentThread().isInterrupted() &&
                           iterator.hasNext()) {
                        if (limit > 0 && ++this.total > limit) {
                            break;
                        }
                        if (++pageCount > pageSize) {
                            long start = System.currentTimeMillis();
                            if (!this.channel.send(dataBuilder)) {
                                if (System.currentTimeMillis() - start >= waitTime * 1000L) {
                                    log.warn(msg, waitTime);
                                    this.timeoutSever();
                                }
                                return;
                            }
                            if (this.isStop.get()) {
                                return;
                            }
                            pageCount = 1;
                            dataBuilder = KvPageRes.newBuilder();
                        }
                        dataBuilder.addData(toKv(kvBuilder, iterator.next(), iterator.position()));
                    }
                    if (this.isStop.get()) {
                        return;
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        this.failServer(HgGrpc.toErr(Status.Code.CANCELLED, "Scanning interrupted"));
                        return;
                    }
                    this.channel.send(dataBuilder);
                } catch (Throwable t) {
                    if (this.isStop.get()) {
                        return;
                    }
                    String msg = "an exception occurred while scanning data:";
                    Status status = t instanceof InterruptedException ||
                                    Thread.currentThread().isInterrupted() ?
                                    Status.CANCELLED : Status.INTERNAL;
                    StatusRuntimeException ex = HgGrpc.toErr(status, msg + t.getMessage(), t);
                    this.failServer(ex);
                } finally {
                    try {
                        if (!this.lifecycle.close(this.iterator)) {
                            this.failServer(Status.INTERNAL.withDescription("Failed to close scan iterator")
                                                           .withCause(this.lifecycle.cleanupFailure())
                                                           .asRuntimeException());
                        }
                    } finally {
                        this.channel.close();
                        synchronized (this.cancellationLock) {
                            this.worker = null;
                        }
                    }
                }

            };
            this.lifecycle.execute(this.executor, scanning);
        } catch (Exception e) {
            StatusRuntimeException ex = e instanceof StatusRuntimeException ?
                                        (StatusRuntimeException) e : HgGrpc.toErr(Status.INTERNAL, null, e);
            this.failServer(ex);
        }

        /*** Scanning loop end ***/
    }

    private Kv toKv(Kv.Builder kvBuilder, RocksDBSession.BackendColumn col,
                    byte[] position) {
        return kvBuilder
                .setKey(ByteString.copyFrom(col.name))
                .setValue(ByteString.copyFrom(col.value))
                .setCode(HgStoreNodeUtil.toInt(position))
                .build();
    }

    private void close() {
        this.cancelServer(true);
    }

    private void next(ScanStreamReq request) {
        this.initIterator(request);
        KvPageRes.Builder resBuilder;

        try {
            if (this.isStop.get()) {
                return;
            }
            resBuilder = this.channel.receive();
            times++;
        } catch (Exception e) {
            if (this.isStop.get()) {
                return;
            }
            String msg = "failed to poll a page of data, cause by:";
            Status status = Thread.currentThread().isInterrupted() ? Status.CANCELLED : Status.INTERNAL;
            log.error(msg, e);
            this.failServer(HgGrpc.toErr(status, msg + e.getMessage(), e));
            return;
        }
        boolean isOver = false;
        if (resBuilder == null || resBuilder.getDataList() == null ||
            resBuilder.getDataList().isEmpty()) {
            isOver = true;
            resBuilder = KvPageRes.newBuilder().addAllData(Collections.EMPTY_LIST);
        }
        this.sendPage(resBuilder.setOver(isOver)
                                .setTimes(times)
                                .setVersion(this.responseVersion)
                                .build());
        if (isOver) {
            this.finishServer();
        }

    }

    private void cancelServer(boolean sendFinalPage) {
        // Cancellation can make the worker throw; establish the normal terminal first.
        boolean complete = this.finishFlag.compareAndSet(false, true);
        this.cancel();
        if (complete) {
            synchronized (this.responseLock) {
                if (sendFinalPage) {
                    this.responseObserver.onNext(KvPageRes.newBuilder()
                                                          .setOver(true)
                                                          .setTimes(++times)
                                                          .setVersion(this.responseVersion)
                                                          .build());
                }
                this.responseObserver.onCompleted();
            }
        }
    }

    private void sendPage(KvPageRes page) {
        synchronized (this.responseLock) {
            if (!this.finishFlag.get()) {
                this.responseObserver.onNext(page);
            }
        }
    }

    private void failServer(Throwable failure) {
        // Claim the terminal signal before waking a receiver on the closed channel.
        boolean reportFailure = this.finishFlag.compareAndSet(false, true);
        this.cancel();
        if (reportFailure) {
            synchronized (this.responseLock) {
                this.responseObserver.onError(failure);
            }
        }
    }

    private void finishServer() {
        if (!this.finishFlag.getAndSet(true)) {
            synchronized (this.responseLock) {
                this.responseObserver.onCompleted();
            }
        }
    }

    private void timeoutSever() {
        String msg = "server wait time exceeds the threshold[" + waitTime +
                     "] seconds.";
        this.failServer(HgGrpc.toErr(Status.Code.DEADLINE_EXCEEDED, msg));
    }

}
