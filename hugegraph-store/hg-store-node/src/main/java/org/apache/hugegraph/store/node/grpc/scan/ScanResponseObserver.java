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

package org.apache.hugegraph.store.node.grpc.scan;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.GraphStoreIterator;
import org.apache.hugegraph.store.grpc.Graphpb.Error;
import org.apache.hugegraph.store.grpc.Graphpb.ErrorType;
import org.apache.hugegraph.store.grpc.Graphpb.ResponseHeader;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.Request;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;

import com.google.protobuf.Descriptors;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScanResponseObserver<T> implements
                                     StreamObserver<ScanPartitionRequest> {

    private static final int BATCH_SIZE = 100000;
    private static final int MAX_PAGE = 8; //
    private static final Error ok = Error.newBuilder().setType(ErrorType.OK).build();
    private static final ResponseHeader okHeader =
            ResponseHeader.newBuilder().setError(ok).build();
    private final BusinessHandler handler;
    private final AtomicInteger nextSeqNo = new AtomicInteger(0);
    private final AtomicInteger cltSeqNo = new AtomicInteger(0);
    private final ThreadPoolExecutor executor;
    private final AtomicBoolean readOver = new AtomicBoolean(false);
    private final LinkedBlockingQueue<ScanResponse> packages =
            new LinkedBlockingQueue(MAX_PAGE * 2);
    private final Descriptors.FieldDescriptor vertexField =
            ScanResponse.getDescriptor().findFieldByNumber(3);
    private final Descriptors.FieldDescriptor edgeField =
            ScanResponse.getDescriptor().findFieldByNumber(4);
    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock sendLock = new ReentrantLock();
    private StreamObserver<ScanResponse> sender;
    private ScanPartitionRequest scanReq;
    private GraphStoreIterator iter;
    private volatile long leftCount;
    private volatile Future<?> sendTask;
    private volatile Future<?> readTask;

    /*
     * November 1, 2022
     * 1. onNext needs to be processed asynchronously to prevent the grpc call from being blocked.
     * 2. Do not read iterators or send data do not produce thread waiting.
     * 3. Before sending, try to prepare the data to be sent as much as possible.
     * */

    /*
     * November 2, 2022
     * 1. Read the thread of rocksdb iterator read
     * 2. Perform data conversion and send to the blocking queue thread offer
     * 3. Thread for reading data from the blocking queue and sending, including waking up the
     * reading and sending threads when no data is read
     * */

    public ScanResponseObserver(StreamObserver<ScanResponse> sender,
                                BusinessHandler handler,
                                ThreadPoolExecutor executor) {
        this.sender = sender;
        this.handler = handler;
        this.executor = executor;
    }

    private boolean readCondition() {
        return packages.remainingCapacity() != 0 && !readOver.get();
    }

    private boolean readTaskCondition() {
        return readCondition() && (readTask == null || readTask.isDone());
    }

    private boolean sendCondition() {
        return nextSeqNo.get() - cltSeqNo.get() < MAX_PAGE;
    }

    private boolean sendTaskCondition() {
        return sendCondition() && (sendTask == null || sendTask.isDone());
    }

    private void offer(Iterable<T> data, boolean isVertex) {
        ScanResponse.Builder builder = ScanResponse.newBuilder();
        builder.setHeader(okHeader).setSeqNo(nextSeqNo.get());
        if (isVertex) {
            builder = builder.setField(vertexField, data);
        } else {
            builder = builder.setField(edgeField, data);
        }
        ScanResponse response = builder.build();
        packages.offer(response);
        startSend();
    }

    private void startRead() {
        if (readTaskCondition()) {
            if (readLock.tryLock()) {
                if (readTaskCondition()) {
                    readTask = executor.submit(rr);
                }
                readLock.unlock();
            }
        }
    }

    private void startSend() {
        if (sendTaskCondition()) {
            if (sendLock.tryLock()) {
                if (sendTaskCondition()) {
                    sendTask = executor.submit(sr);
                }
                sendLock.unlock();
            }
        }
    }

    @Override
    public void onNext(ScanPartitionRequest scanReq) {
        if (scanReq.hasScanRequest() && !scanReq.hasReplyRequest()) {
            this.scanReq = scanReq;
            Request request = scanReq.getScanRequest();
            long rl = request.getLimit();
            leftCount = rl > 0 ? rl : Long.MAX_VALUE;
            iter = handler.scan(scanReq);
            if (!iter.hasNext()) {
                close();
                sender.onCompleted();
            } else {
                readTask = executor.submit(rr);
            }
        } else {
            cltSeqNo.getAndIncrement();
            startSend();
        }
    }

    @Override
    public void onError(Throwable t) {
        close();
        log.warn("receive client error:", t);
    }

    @Override
    public void onCompleted() {
        close();
    }

    private void close() {
        try {
            nextSeqNo.set(0);
            if (sendTask != null) {
                sendTask.cancel(true);
            }
            if (readTask != null) {
                readTask.cancel(true);
            }
            readOver.set(true);
            iter.close();
        } catch (Exception e) {
            log.warn("on Complete with error:", e);
        }
    }

    Runnable rr = new Runnable() {
        @Override
        public void run() {
            try {
                if (readCondition()) {
                    synchronized (iter) {
                        while (readCondition()) {
                            Request r = scanReq.getScanRequest();
                            ScanType t = r.getScanType();
                            boolean isVertex = t.equals(ScanType.SCAN_VERTEX);
                            ArrayList<T> data = new ArrayList<>(BATCH_SIZE);
                            int count = 0;
                            while (iter.hasNext() && leftCount > -1) {
                                count++;
                                leftCount--;
                                T next = (T) iter.next();
                                data.add(next);
                                if (count >= BATCH_SIZE) {
                                    offer(data, isVertex);
                                    // data.clear();
                                    break;
                                }
                            }
                            if (!(iter.hasNext() && leftCount > -1)) {
                                if (data.size() > 0 &&
                                    data.size() < BATCH_SIZE) {
                                    offer(data, isVertex);
                                }
                                readOver.set(true);
                                data = null;
                                //log.warn("scan complete , count: {},time: {}",
                                //         sum, System.currentTimeMillis());
                                return;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("read data with error: ", e);
                sender.onError(e);
            }
        }
    };

    Runnable sr = () -> {
        while (sendCondition()) {
            ScanResponse response;
            try {
                if (readOver.get()) {
                    if ((response = packages.poll()) == null) {
                        sender.onCompleted();
                    } else {
                        sender.onNext(response);
                        nextSeqNo.incrementAndGet();
                    }
                } else {
                    response = packages.poll(10,
                                             TimeUnit.MILLISECONDS);
                    if (response != null) {
                        sender.onNext(response);
                        nextSeqNo.incrementAndGet();
                        startRead();
                    } else {
                        break;
                    }
                }

            } catch (InterruptedException e) {
                break;
            }
        }
    };
}
