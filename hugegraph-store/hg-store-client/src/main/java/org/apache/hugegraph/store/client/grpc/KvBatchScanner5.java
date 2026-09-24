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

package org.apache.hugegraph.store.client.grpc;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.client.util.Base58;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.ScanCancelRequest;
import org.apache.hugegraph.store.grpc.stream.ScanReceiptRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2022/03/23
 *
 * @version 5.0.0
 */
@Slf4j
@NotThreadSafe
class KvBatchScanner5 {

    private final static HgStoreClientConfig storeClientConfig = HgStoreClientConfig.of();
    //private final static int HAVE_NEXT_TIMEOUT_SECONDS = storeClientConfig
    // .getNetKvScannerHaveNextTimeout();
    private final static int HAVE_NEXT_TIMEOUT_SECONDS = 60;
    private final static long PAGE_SIZE = storeClientConfig.getNetKvScannerPageSize();

    public static KvCloseableIterator scan(HgStoreNodeSession nodeSession,
                                           HgStoreStreamGrpc.HgStoreStreamStub stub,
                                           HgScanQuery scanQuery) {
        return new OrderConsumer(new OrderBroker(stub, scanQuery, nodeSession));
    }

    private enum OrderState {
        NEW(0),
        WORKING(1),
        COMPLETED(10);

        int value;

        OrderState(int value) {
            this.value = value;
        }
    }

    /*** Broker ***/
    private static class OrderBroker {

        public final OrderKeeper keeper = new OrderKeeper();
        private final HgScanQuery scanQuery;
        private final StreamObserver<ScanStreamBatchReq> requestObserver;
        private final ScanStreamBatchReq.Builder reqBuilder;
        private final ReentrantLock senderLock = new ReentrantLock();
        private final AtomicBoolean serverFinished = new AtomicBoolean();
        private final AtomicBoolean clientFinished = new AtomicBoolean();
        private final ScanReceiptRequest.Builder receiptReqBuilder =
                ScanReceiptRequest.newBuilder();
        private final ScanCancelRequest cancelReq = ScanCancelRequest.newBuilder().build();
        private final HgStoreNodeSession nodeSession;
        private final OrderAgent agent;
        private final AtomicLong receivedCount = new AtomicLong();
        private final AtomicInteger receivedLastTimes = new AtomicInteger();
        private final BlockingQueue<Integer> timesQueue = new LinkedBlockingQueue();
        String brokerId = "";
        private OrderState state = OrderState.NEW;

        OrderBroker(HgStoreStreamGrpc.HgStoreStreamStub stub,
                    HgScanQuery scanQuery,
                    HgStoreNodeSession nodeSession) {

            if (log.isDebugEnabled()) {
                if (scanQuery.getPrefixList() != null && scanQuery.getPrefixList().size() > 0) {
                    brokerId = Base58.encode(scanQuery.getPrefixList().get(0).getKey());

                    log.debug(
                            "[ANALYSIS START] [{}] firstKey: {}, keyLength: {}, table: {}, node: {}"
                            , brokerId
                            , scanQuery.getPrefixList().get(0)
                            , scanQuery.getPrefixList().size()
                            , scanQuery.getTable()
                            , nodeSession.getStoreNode().getAddress());
                }
            }

            this.scanQuery = scanQuery;
            this.reqBuilder = KvBatchUtil.getRequestBuilder(nodeSession);
            this.nodeSession = nodeSession;
            this.agent = new OrderAgent(brokerId);
            this.requestObserver = stub.scanBatch(agent);

        }

        List<Kv> oneMore() {

            if (this.state == OrderState.NEW) {
                synchronized (this.state) {
                    if (this.state == OrderState.NEW) {
                        this.makeADeal();
                        this.state = OrderState.WORKING;
                    }
                }
            } else {
                this.sendReceipt();
            }

            return this.keeper.pickUp();
        }

        void receipt(int times) {
            this.timesQueue.offer(times);
            receivedLastTimes.set(times);
        }

        void sendReceipt() {
            Integer buf = this.timesQueue.poll();

            if (buf == null) {
                buf = this.receivedLastTimes.get();
            }

            AtomicInteger timesBuf = new AtomicInteger(buf);

            if (!this.clientFinished.get()) {
                this.send(() ->
                                  getReqBuilder().setReceiptRequest(
                                                         this.receiptReqBuilder.setTimes(timesBuf.get()).build())
                                                 .build()
                );
            }
        }

        private void makeADeal() {
            this.send(() -> getReqBuilder()
                    .setQueryRequest(KvBatchUtil.createQueryReq(scanQuery, PAGE_SIZE)).build()
            );
        }

        private void finish(long tookAmt) {
            this.clientFinished.set(true);
            if (log.isDebugEnabled()) {
                log.debug("[ANALYSIS END] [{}] times: {}, received: {}, took: {}"
                        , this.brokerId
                        , this.receivedLastTimes.get()
                        , this.receivedCount.get()
                        , tookAmt
                );
            }
            if (this.receivedCount.get() != tookAmt) {
                if (log.isDebugEnabled()) {
                    log.debug("[ANALYSIS END] [{}] times: {}, received: {}, took: {}"
                            , this.brokerId
                            , this.receivedLastTimes.get()
                            , this.receivedCount.get()
                            , tookAmt
                    );
                }
            }
            synchronized (this.state) {
                if (this.state.value < OrderState.COMPLETED.value) {
                    this.send(() -> getReqBuilder().setCancelRequest(this.cancelReq).build());
                    this.state = OrderState.COMPLETED;
                }
            }
        }

        private ScanStreamBatchReq.Builder getReqBuilder() {
            return this.reqBuilder.clearQueryRequest();
        }

        private void send(Supplier<ScanStreamBatchReq> supplier) {
            this.senderLock.lock();
            try {
                if (!this.serverFinished.get()) {
                    this.requestObserver.onNext(supplier.get());
                }
                Thread.yield();
            } finally {
                this.senderLock.unlock();
            }
        }

        private class OrderAgent implements StreamObserver<KvPageRes> {

            private final AtomicInteger count = new AtomicInteger(0);
            private final AtomicBoolean over = new AtomicBoolean(false);
            private final String agentId;

            OrderAgent(String agentId) {
                this.agentId = agentId;
            }

            @Override
            public void onNext(KvPageRes value) {
                if (log.isDebugEnabled()) {
                    log.debug("Scan [ {} ] [ {} ] times, received: [ {} ]"
                            , nodeSession.getStoreNode().getAddress(), value.getTimes(),
                              value.getDataList().size());
                }

                serverFinished.set(value.getOver());

                List<Kv> buffer = value.getDataList();
                count.addAndGet(buffer.size());
                if (log.isDebugEnabled()) {
                    if (value.getOver()) {
                        log.debug("[ANALYSIS OVER] [{}] count: {}", agentId, count);
                    }
                }
                keeper.receive(buffer, value.getTimes());
                this.over.set(value.getOver());
                this.checkOver(value.getTimes());
            }

            private void checkOver(int times) {
                if (this.over.get()) {
                    requestObserver.onCompleted();
                    keeper.done(times);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("received server onError event, Throwable:", t);
                keeper.shout(t);
            }

            @Override
            public void onCompleted() {
                if (log.isDebugEnabled()) {
                    log.debug("received sever completed event.");
                }
                serverFinished.set(true);

            }

        }

        /*** Inventory Keeper ***/
        private class OrderKeeper {

            private final BlockingQueue<Supplier<List<Kv>>> queue = new LinkedBlockingQueue<>();
            private final ReentrantLock pickUpLock = new ReentrantLock();
            private final AtomicBoolean done = new AtomicBoolean();
            private final AtomicBoolean stop = new AtomicBoolean();
            private int timesOfOver;
            private int lastTimes;
            private Throwable serverErr;

            void receive(List<Kv> data, int times) {
                receivedCount.addAndGet(data.size());
                this.queue.offer(() -> data);
                receipt(times);

                this.lastTimes = times;
            }

            private List<Kv> pickUp() {
                Supplier<List<Kv>> res;

                pickUpLock.lock();
                try {

                    if (this.done.get()) {
                        if (this.stop.get()) {
                            log.warn("Invoking pickUp method after OrderKeeper has bean closing.");
                        }
                        res = this.queue.poll();
                        if (res == null) {
                            res = () -> null;
                        }
                    } else {
                        res = this.queue.poll(HAVE_NEXT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (res == null) {
                            if (this.done.get()) {
                                res = () -> null;
                            } else {
                                throw HgStoreClientException.of(
                                        "Timeout, max time: " + HAVE_NEXT_TIMEOUT_SECONDS +
                                        " seconds" +
                                        ", isOver: " + this.done.get() +
                                        ", isStop: " + this.stop.get() +
                                        ", last-times: " + this.lastTimes +
                                        ", over-times: " + this.timesOfOver);
                            }
                        }

                    }
                } catch (InterruptedException e) {
                    log.error(
                            "Failed to receive List<Kv> from queue because of interruption of " +
                            "current thread ["
                            + Thread.currentThread().getName() + "]");

                    Thread.currentThread().interrupt();

                    throw HgStoreClientException.of(
                            "Failed to receive List<Kv> from queue, cause by:", e);
                } finally {
                    pickUpLock.unlock();
                }

                checkServerErr();
                return res.get();

            }

            void done(int times) {
                this.timesOfOver = times;
                this.done.set(true);
                this.queue.offer(() -> null);
            }

            void shout(Throwable t) {
                this.serverErr = t;
                log.error("Failed to receive from sever", t);
                this.queue.offer(() -> null);
            }

            private void checkServerErr() {
                if (this.serverErr != null) {
                    throw HgStoreClientException.of(this.serverErr);
                }
            }
        }

    }

    /* iterator */
    private static class OrderConsumer implements KvCloseableIterator<Kv>, HgPageSize {

        private final OrderBroker broker;
        private final String consumerId;
        private Iterator<Kv> dataIterator;
        private long tookCount = 0;

        OrderConsumer(OrderBroker broker) {
            this.broker = broker;
            consumerId = broker.brokerId;
        }

        private Iterator<Kv> getIterator() {
            List<Kv> list = this.broker.oneMore();

            if (log.isDebugEnabled()) {
                if (list != null && list.isEmpty()) {
                    log.debug("[ANALYSIS EMPTY] [{}] , tookCount: {}", consumerId, tookCount);
                }
            }

            if (list == null || list.isEmpty()) {
                return null;
            } else {
                return list.iterator();
            }
        }

        @Override
        public void close() {
            this.broker.finish(this.tookCount);
        }

        @Override
        public long getPageSize() {
            return PAGE_SIZE;
        }

        @Override
        public boolean hasNext() {

            if (this.dataIterator == null) {
                this.dataIterator = this.getIterator();
            } else {
                if (this.dataIterator.hasNext()) {
                    return true;
                } else {
                    this.dataIterator = this.getIterator();
                }
            }

            if (this.dataIterator == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[ANALYSIS NULL -> FALSE] [{}] , tookCount: {}", consumerId,
                              tookCount);
                }
                return false;
            } else {
                if (log.isDebugEnabled()) {
                    if (!this.dataIterator.hasNext()) {
                        log.debug("[ANALYSIS hasNext -> FALSE] [{}] , tookCount: {}", consumerId,
                                  tookCount);
                    }
                }
                return this.dataIterator.hasNext();
            }

        }

        @Override
        public Kv next() {
            if (this.dataIterator == null) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
            }

            if (log.isDebugEnabled()) {
                tookCount++;
                if (tookCount % 10000 == 0) {
                    log.debug("[ANALYSIS NEXT] [{}] , tookCount: {}", consumerId, tookCount);
                }
            }
            return this.dataIterator.next();
        }

    }

}
