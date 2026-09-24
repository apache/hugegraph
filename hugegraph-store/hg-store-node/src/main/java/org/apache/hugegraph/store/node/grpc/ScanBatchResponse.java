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

import static org.apache.hugegraph.store.node.grpc.ScanUtil.getParallelIterator;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.buffer.ByteBufferAllocator;
import org.apache.hugegraph.store.buffer.KVByteBuffer;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanQueryRequest;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.node.util.HgGrpc;
import org.apache.hugegraph.store.node.util.PropertyUtil;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * Batch query processor, batch query data, stream back data.
 * 1. Server-side streaming data to the client
 * 2. The client returns the batch number to the server after consuming each batch of data.
 * 3. The server decides how much data to send based on the batch number, ensuring the
 * uninterrupted transmission of data,
 */
@Slf4j
public class ScanBatchResponse implements StreamObserver<ScanStreamBatchReq> {

    static ByteBufferAllocator bfAllocator =
            new ByteBufferAllocator(ParallelScanIterator.maxBodySize * 3 / 2, 1000);
    static ByteBufferAllocator alloc =
            new ByteBufferAllocator(ParallelScanIterator.maxBodySize * 3 / 2, 1000);
    private final int maxInFlightCount = PropertyUtil.getInt("app.scan.stream.inflight", 16);
    private final StreamObserver<KvStream> sender;
    // unit: second
    private final int activeTimeout = PropertyUtil.getInt("app.scan.stream.timeout", 60);
    private final HgStoreWrapperEx wrapper;
    private final ThreadPoolExecutor executor;
    private final Object stateLock = new Object();
    private final Lock iteratorLock = new ReentrantLock();
    // Currently traversing iterator
    private ScanIterator iterator;
    // Next send sequence number
    private volatile int seqNo;
    // Client consumed sequence number
    private volatile int clientSeqNo;
    // Number of entries sent
    private volatile long count;
    // Client requests the maximum number of entries to return
    private volatile long limit;
    private ScanQueryRequest query;
    // Last read data time
    private long activeTime;
    private volatile State state;

    public ScanBatchResponse(StreamObserver<KvStream> response, HgStoreWrapperEx wrapper,
                             ThreadPoolExecutor executor) {
        this.sender = response;
        this.wrapper = wrapper;
        this.executor = executor;
        this.iterator = null;
        this.seqNo = 1;
        this.state = State.IDLE;
        this.activeTime = System.currentTimeMillis();
    }

    /**
     * Receive messages sent by the client
     * Server starts a new thread to process messages, does not block the network.
     *
     * @param request
     */
    @Override
    public void onNext(ScanStreamBatchReq request) {
        switch (request.getQueryCase()) {
            case QUERY_REQUEST: // query conditions
                executor.execute(() -> {
                    startQuery(request.getHeader().getGraph(), request.getQueryRequest());
                });
                break;
            case RECEIPT_REQUEST:   // Message asynchronous response
                this.clientSeqNo = request.getReceiptRequest().getTimes();
                if (seqNo - clientSeqNo < maxInFlightCount) {
                    synchronized (stateLock) {
                        if (state == State.IDLE) {
                            state = State.DOING;
                            executor.execute(() -> {
                                sendEntries();
                            });
                        } else if (state == State.DONE) {
                            sendNoDataEntries();
                        }
                    }
                }
                break;
            case CANCEL_REQUEST: // close stream
                closeQuery();
                break;
            default:
                sender.onError(
                        HgGrpc.toErr("Unsupported sub-request: [ " + request + " ]"));
        }
    }

    @Override
    public void onError(Throwable t) {
        log.error("onError ", t);
        closeQuery();
    }

    @Override
    public void onCompleted() {
        closeQuery();
    }

    /**
     * Generate iterator
     *
     * @param request
     */
    private void startQuery(String graphName, ScanQueryRequest request) {
        this.query = request;
        this.limit = request.getLimit();
        this.count = 0;
        this.iterator = getParallelIterator(graphName, request, this.wrapper, executor);
        synchronized (stateLock) {
            if (state == State.IDLE) {
                state = State.DOING;
                executor.execute(() -> {
                    sendEntries();
                });
            }
        }
    }

    /**
     * Generate iterator
     */
    private void closeQuery() {
        setStateDone();
        try {
            closeIter();
            this.sender.onCompleted();
        } catch (Exception e) {
            log.error("exception ", e);
        }
        int active = ScanBatchResponseFactory.getInstance().removeStreamObserver(this);
        log.info("ScanBatchResponse closeQuery, active count is {}", active);
    }

    private void closeIter() {
        try {
            if (this.iterator != null) {
                this.iterator.close();
                this.iterator = null;
            }
        } catch (Exception e) {

        }
    }

    /**
     * Send data
     */
    private void sendEntries() {
        if (state == State.DONE || iterator == null) {
            setStateIdle();
            return;
        }
        iteratorLock.lock();
        try {
            if (state == State.DONE || iterator == null) {
                setStateIdle();
                return;
            }
            KvStream.Builder dataBuilder = KvStream.newBuilder().setVersion(1);
            while (state != State.DONE && iterator.hasNext()
                   && (seqNo - clientSeqNo < maxInFlightCount)
                   && this.count < limit) {
                KVByteBuffer buffer = new KVByteBuffer(alloc.get());
                List<ParallelScanIterator.KV> dataList = iterator.next();
                dataList.forEach(kv -> {
                    kv.write(buffer);
                    this.count++;
                });
                dataBuilder.setStream(buffer.flip().getBuffer());
                dataBuilder.setSeqNo(seqNo++);
                dataBuilder.complete(e -> alloc.release(buffer.getBuffer()));
                this.sender.onNext(dataBuilder.build());
                this.activeTime = System.currentTimeMillis();
            }
            if (!iterator.hasNext() || this.count >= limit || state == State.DONE) {
                closeIter();
                this.sender.onNext(KvStream.newBuilder().setOver(true).build());
                setStateDone();
            } else {
                setStateIdle();
            }
        } catch (Throwable e) {
            if (this.state != State.DONE) {
                log.error(" send data exception: ", e);
                setStateIdle();
                if (this.sender != null) {
                    try {
                        this.sender.onError(e);
                    } catch (Exception ex) {
                        log.warn("Error when call sender.onError {}", e.getMessage());
                    }
                }
            }
        } finally {
            iteratorLock.unlock();
        }
    }

    private void sendNoDataEntries() {
        try {
            this.sender.onNext(KvStream.newBuilder().setOver(true).build());
        } catch (Exception e) {
        }
    }

    private State setStateDone() {
        synchronized (this.stateLock) {
            this.state = State.DONE;
        }
        return state;
    }

    private State setStateIdle() {
        synchronized (this.stateLock) {
            if (this.state != State.DONE) {
                this.state = State.IDLE;
            }
        }
        return state;
    }

    /**
     * Check for activity, if the client does not request data for a certain period of time, it
     * is considered inactive, close the connection to release resources.
     */
    public void checkActiveTimeout() {
        if ((System.currentTimeMillis() - activeTime) > activeTimeout * 1000L) {
            log.warn("The stream is not closed, and the timeout is forced to close");
            closeQuery();
        }
    }

    /**
     * Task Status
     */
    private enum State {
        IDLE,
        DOING,
        DONE,
        ERROR
    }
}
