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

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanCondition;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.node.util.HgAssert;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.HgStoreConst;
import org.apache.hugegraph.store.node.util.HgStoreNodeUtil;
import org.apache.hugegraph.store.util.Base58Encoder;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/03/27
 *
 * @version 3.6.0
 */
@Slf4j
public class ScanBatchResponse3 {

    private final static long DEFAULT_PACKAGE_SIZE = 10_000;
    private final static int MAX_NOT_RECEIPT = 10;

    public static StreamObserver of(StreamObserver<KvPageRes> responseObserver,
                                    HgStoreWrapperEx wrapper, ThreadPoolExecutor executor) {
        HgAssert.isArgumentNotNull(responseObserver, "responseObserver");
        HgAssert.isArgumentNotNull(wrapper, "wrapper");
        return new Broker(responseObserver, wrapper, executor);
    }

    private enum OrderState {
        NEW(0),
        WORKING(1);//, PAUSE(2), COMPLETE(10);
        int value;

        OrderState(int value) {
            this.value = value;
        }
    }

    /*** Broker ***/
    private static class Broker implements StreamObserver<ScanStreamBatchReq> {

        private final StreamObserver<KvPageRes> responseObserver;
        private final HgStoreWrapperEx wrapper;
        private final ThreadPoolExecutor executor;
        private final OrderManager manager = new OrderManager();
        private String graph;

        Broker(StreamObserver<KvPageRes> responseObserver, HgStoreWrapperEx wrapper,
               ThreadPoolExecutor executor) {
            this.responseObserver = responseObserver;
            this.wrapper = wrapper;
            this.executor = executor;
        }

        @Override
        public void onNext(ScanStreamBatchReq request) {
            this.handleHeader(request);
            switch (request.getQueryCase()) {
                case QUERY_REQUEST:
                    this.makeADeal(request.getQueryRequest());
                    break;
                case RECEIPT_REQUEST:
                    this.manager.receipt(request.getReceiptRequest().getTimes());
                    break;
                case CANCEL_REQUEST:
                    this.manager.finished();
                    break;
                default:
                    responseObserver.onError(
                            HgGrpc.toErr("Unsupported sub-request: [ " + request + " ]"));
            }
        }

        @Override
        public void onError(Throwable t) {
            log.warn(t.getMessage());
            this.manager.breakdown();
        }

        @Override
        public void onCompleted() {
            this.manager.finished();
        }

        private void handleHeader(ScanStreamBatchReq request) {
            if (this.graph == null) {
                this.graph = request.getHeader().getGraph();
            }
        }

        private void makeADeal(ScanQueryRequest request) {
            String deliverId = "";
            if (log.isDebugEnabled()) {
                List<ScanCondition> conditions = request.getConditionList();
                if (conditions.size() > 0) {
                    ScanCondition c = conditions.get(0);
                    if (c.getPrefix() != null && c.getPrefix().size() > 0) {
                        deliverId = Base58Encoder.convertToBase58(c.getPrefix().toByteArray());
                        log.info("[ANALYSIS DEAL] [{}] prefixLength: {}", deliverId,
                                 conditions.size());
                    }

                }
            }

            OrderDeliverer deliverer = new OrderDeliverer(deliverId, this.responseObserver);
            OrderWorker worker = new OrderWorker(
                    request.getLimit(),
                    request.getPageSize(),
                    ScanUtil.getIterator(this.graph, request, this.wrapper),
                    deliverer,
                    this.executor);

            this.manager.deal(worker, deliverer);
        }

    }

    @NotThreadSafe
    private static class OrderManager {

        OrderState state = OrderState.NEW;
        OrderWorker worker;
        OrderDeliverer deliverer;

        synchronized void deal(OrderWorker worker, OrderDeliverer deliverer) {
            if (log.isDebugEnabled()) {
                log.debug("Receiving query request.");
            }
            if (this.state == OrderState.NEW) {
                this.worker = worker;
                this.deliverer = deliverer;
                this.worker.hereWeGo();
                this.state = OrderState.WORKING;
            }
        }

        synchronized void receipt(int receiptTimes) {
            if (log.isDebugEnabled()) {
                log.debug("Receiving receipt request.");
            }
            this.worker.setReceipt(receiptTimes);
        }

        synchronized void finished() {
            if (log.isDebugEnabled()) {
                log.debug("Receiving finished request.");
            }
            this.breakdown();
        }

        synchronized void breakdown() {
            if (this.worker != null) {
                this.worker.breakdown();
            }
        }
    }

    private static class OrderDeliverer {

        private final StreamObserver<KvPageRes> responseObserver;
        private final AtomicBoolean finishFlag = new AtomicBoolean();
        private final String delivererId;
        private final AtomicLong count = new AtomicLong();

        OrderDeliverer(String delivererId, StreamObserver<KvPageRes> responseObserver) {
            this.responseObserver = responseObserver;
            this.delivererId = delivererId;
        }

        void deliver(KvPageRes.Builder dataBuilder, int times, boolean isOver) {
            if (this.finishFlag.get()) {
                return;
            }
            count.addAndGet(dataBuilder.getDataCount());
            this.responseObserver.onNext(dataBuilder.setOver(isOver).setTimes(times).build());
            if (log.isDebugEnabled()) {
                log.debug("deliver times : {}, over: {}", times, isOver);
            }

            if (isOver) {
                if (log.isDebugEnabled()) {
                    if (delivererId != null && !delivererId.isEmpty()) {
                        log.debug("[ANALYSIS OVER] [{}] count: {}, times: {}", delivererId, count,
                                  times);
                    }
                }
                this.finish();
            }
        }

        void finish() {
            if (!finishFlag.getAndSet(true)) {
                this.responseObserver.onCompleted();
            }
        }

        void error(String msg) {
            if (!finishFlag.getAndSet(true)) {
                this.responseObserver.onError(HgGrpc.toErr(msg));
            }
        }

        void error(String msg, Throwable t) {
            if (!finishFlag.getAndSet(true)) {
                this.responseObserver.onError(HgGrpc.toErr(Status.INTERNAL,
                                                           msg, t));
            }
        }
    }

    /*** Worker ***/
    private static class OrderWorker {

        private final ScanIterator iterator;
        private final OrderDeliverer deliverer;
        private final AtomicBoolean pauseFlag = new AtomicBoolean();
        private final AtomicBoolean completeFlag = new AtomicBoolean();
        private final ReentrantLock workingLock = new ReentrantLock();
        private final AtomicBoolean isWorking = new AtomicBoolean();
        private final AtomicBoolean breakdown = new AtomicBoolean();
        private final AtomicInteger receiptTimes = new AtomicInteger();
        private final AtomicInteger curTimes = new AtomicInteger();
        private final ThreadPoolExecutor executor;
        private final long limit;
        private long packageSize;
        private long counter;

        OrderWorker(long limit, long packageSize, ScanIterator iterator, OrderDeliverer deliverer,
                    ThreadPoolExecutor executor) {
            this.limit = limit;
            this.packageSize = packageSize;
            this.iterator = iterator;
            this.deliverer = deliverer;
            this.executor = executor;

            if (this.packageSize <= 0) {
                this.packageSize = DEFAULT_PACKAGE_SIZE;
                log.warn(
                        "As page-Size is less than or equals 0, default package-size was " +
                        "effective.[ {} ]",
                        DEFAULT_PACKAGE_SIZE);
            }

        }

        void hereWeGo() {
            if (this.completeFlag.get()) {
                log.warn("job complete.");
                return;
            }
            if (this.isWorking.get()) {
                log.warn("has been working.");
                return;
            }

            if (this.workingLock.isLocked()) {
                log.warn("working now");
                return;
            }

            executor.execute(() -> working());
            Thread.yield();
        }

        void setReceipt(int times) {
            this.receiptTimes.set(times);
            this.continueWorking();
        }

        boolean checkContinue() {
            return (this.curTimes.get() - this.receiptTimes.get() < MAX_NOT_RECEIPT);
        }

        void continueWorking() {
            if (this.checkContinue()) {
                synchronized (this.iterator) {
                    this.iterator.notify();
                }
            }
        }

        void breakdown() {
            this.breakdown.set(true);
            synchronized (this.iterator) {
                this.iterator.notify();
            }
        }

        private void working() {
            if (this.isWorking.getAndSet(true)) {
                return;
            }

            this.workingLock.lock();

            try {
                synchronized (this.iterator) {
                    KvPageRes.Builder dataBuilder = KvPageRes.newBuilder();
                    Kv.Builder kvBuilder = Kv.newBuilder();
                    long packageCount = 0;

                    while (iterator.hasNext()) {
                        if (++this.counter > limit) {
                            this.completeFlag.set(true);
                            break;
                        }

                        if (++packageCount > packageSize) {

                            if (this.breakdown.get()) {
                                break;
                            }

                            deliverer.deliver(dataBuilder, curTimes.incrementAndGet(), false);
                            Thread.yield();

                            if (!this.checkContinue()) {
                                long start = System.currentTimeMillis();
                                iterator.wait(
                                        HgStoreConst.SCAN_WAIT_CLIENT_TAKING_TIME_OUT_SECONDS *
                                        1000);

                                if (System.currentTimeMillis() - start
                                    >=
                                    HgStoreConst.SCAN_WAIT_CLIENT_TAKING_TIME_OUT_SECONDS * 1000) {
                                    throw new TimeoutException("Waiting continue more than "
                                                               +
                                                               HgStoreConst.SCAN_WAIT_CLIENT_TAKING_TIME_OUT_SECONDS +
                                                               " seconds.");
                                }

                                if (this.breakdown.get()) {
                                    break;
                                }

                            }

                            packageCount = 1;
                            dataBuilder = KvPageRes.newBuilder();
                        }

                        RocksDBSession.BackendColumn col = iterator.next();

                        dataBuilder.addData(kvBuilder
                                                    .setKey(ByteString.copyFrom(col.name))
                                                    .setValue(ByteString.copyFrom(col.value))
                                                    .setCode(HgStoreNodeUtil.toInt(
                                                            iterator.position()))
//position == partition-id.
                        );

                    }

                    this.completeFlag.set(true);

                    deliverer.deliver(dataBuilder, curTimes.incrementAndGet(), true);

                }

            } catch (InterruptedException e) {
                log.error("Interrupted waiting of iterator, canceled while.", e);
                this.deliverer.error("Failed to finish scanning, cause by InterruptedException.");
            } catch (TimeoutException t) {
                log.info(t.getMessage());
                this.deliverer.error("Sever waiting exceeded ["
                                     + HgStoreConst.SCAN_WAIT_CLIENT_TAKING_TIME_OUT_SECONDS +
                                     "] seconds.");
            } catch (Throwable t) {
                log.error("Failed to do while for scanning, cause by:", t);
                this.deliverer.error("Failed to finish scanning ", t);
            } finally {
                this.workingLock.unlock();
                this.iterator.close();
            }
        }

    }

}
