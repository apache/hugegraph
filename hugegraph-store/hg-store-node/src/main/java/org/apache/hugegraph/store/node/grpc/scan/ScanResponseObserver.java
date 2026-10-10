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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.GraphStoreIterator;
import org.apache.hugegraph.store.grpc.Graphpb;
import org.apache.hugegraph.store.grpc.Graphpb.ErrorType;
import org.apache.hugegraph.store.grpc.Graphpb.ResponseHeader;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest.ScanType;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;

import com.google.protobuf.Descriptors;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class ScanResponseObserver<T> implements StreamObserver<ScanPartitionRequest> {

    private static final int BATCH_SIZE = 100000;
    private static final int MAX_PAGE = 8;
    private static final ResponseHeader OK = ResponseHeader.newBuilder()
            .setError(Graphpb.Error.newBuilder().setType(ErrorType.OK)).build();
    private final BusinessHandler handler;
    private final StreamObserver<ScanResponse> sender;
    private final Consumer<Runnable> execute;
    private final Consumer<Throwable> failedCleanup;
    private final AtomicInteger nextSeqNo = new AtomicInteger();
    private final AtomicInteger cltSeqNo = new AtomicInteger();
    private final AtomicBoolean readOver = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean halfClosed = new AtomicBoolean();
    private final AtomicBoolean reading = new AtomicBoolean();
    private final AtomicBoolean sending = new AtomicBoolean();
    private final LinkedBlockingQueue<ScanResponse> packages = new LinkedBlockingQueue<>(MAX_PAGE * 2);
    private final Object iteratorLock = new Object();
    private final Object readerLock = new Object();
    private Thread reader;
    private GraphStoreIterator<T> iter;
    private ScanPartitionRequest scanReq;
    private boolean iteratorClosed;
    private long leftCount;

    public ScanResponseObserver(StreamObserver<ScanResponse> sender,
                                BusinessHandler handler, ThreadPoolExecutor executor) {
        this(sender, handler, executor::execute, failure -> { });
    }

    public ScanResponseObserver(StreamObserver<ScanResponse> sender,
                                BusinessHandler handler,
                                Consumer<Runnable> execute, Consumer<Throwable> failedCleanup) {
        this.sender = sender;
        this.handler = handler;
        this.execute = execute;
        this.failedCleanup = failedCleanup;
    }

    @Override
    public void onNext(ScanPartitionRequest request) {
        if (this.closed.get() || this.halfClosed.get()) {
            return;
        }
        try {
            if (request.hasScanRequest() && !request.hasReplyRequest()) {
                synchronized (this.iteratorLock) {
                    if (this.closed.get() || this.halfClosed.get()) {
                        return;
                    }
                    if (this.iter != null) {
                        throw Status.INVALID_ARGUMENT.withDescription("Scan request was already initialized")
                                                     .asRuntimeException();
                    }
                    this.scanReq = request;
                    long limit = request.getScanRequest().getLimit();
                    this.leftCount = limit > 0 ? limit : Long.MAX_VALUE;
                    this.iter = this.handler.scan(request);
                }
                startRead();
            } else {
                this.cltSeqNo.incrementAndGet();
                startSend();
            }
        } catch (RuntimeException | Error failure) {
            terminate(failure);
        }
    }

    @Override
    public void onError(Throwable failure) {
        terminate(failure);
    }

    @Override
    public void onCompleted() {
        boolean initialized;
        synchronized (this.iteratorLock) {
            this.halfClosed.set(true);
            initialized = this.iter != null;
        }
        if (!initialized) {
            terminate(null);
        } else {
            startSend();
        }
    }

    private boolean readCondition() {
        return !this.closed.get() && !this.readOver.get() && this.packages.remainingCapacity() > 0;
    }

    private boolean sendCondition() {
        return !this.closed.get() && this.nextSeqNo.get() - this.cltSeqNo.get() < MAX_PAGE;
    }

    private boolean sendWorkAvailable() {
        return !this.closed.get() &&
               (sendCondition() || this.halfClosed.get() ||
                (this.readOver.get() && this.packages.isEmpty()));
    }

    private void startRead() {
        if (!readCondition() || !this.reading.compareAndSet(false, true)) {
            return;
        }
        try {
            this.execute.accept(() -> {
                try {
                    synchronized (this.readerLock) {
                        if (this.closed.get()) {
                            return;
                        }
                        this.reader = Thread.currentThread();
                    }
                    read();
                } catch (RuntimeException | Error failure) {
                    terminate(failure);
                } finally {
                    synchronized (this.readerLock) {
                        if (this.reader == Thread.currentThread()) {
                            this.reader = null;
                        }
                    }
                    this.reading.set(false);
                    if (readCondition()) {
                        startRead();
                    }
                }
            });
        } catch (RuntimeException | Error failure) {
            this.reading.set(false);
            terminate(failure);
        }
    }

    private void read() {
        synchronized (this.iteratorLock) {
            while (readCondition()) {
                ArrayList<T> data = new ArrayList<>(BATCH_SIZE);
                while (!this.closed.get() && this.leftCount > 0 &&
                       data.size() < BATCH_SIZE && this.iter.hasNext()) {
                    data.add(this.iter.next());
                    this.leftCount--;
                }
                if (this.closed.get()) {
                    return;
                }
                boolean ended = this.leftCount == 0 || !this.iter.hasNext();
                if (!data.isEmpty()) {
                    Descriptors.FieldDescriptor field = ScanResponse.getDescriptor().findFieldByNumber(
                            this.scanReq.getScanRequest().getScanType() == ScanType.SCAN_VERTEX ? 3 : 4);
                    this.packages.add(ScanResponse.newBuilder().setHeader(OK)
                                                 .setSeqNo(this.nextSeqNo.get()).setField(field, data).build());
                }
                if (ended) {
                    closeIterator();
                    this.readOver.set(true);
                }
                startSend();
            }
        }
    }

    private void startSend() {
        if (!sendWorkAvailable() || !this.sending.compareAndSet(false, true)) {
            return;
        }
        try {
            this.execute.accept(() -> {
                try {
                    while (!this.closed.get()) {
                        if (this.readOver.get() && this.packages.isEmpty()) {
                            terminate(null);
                            return;
                        }
                        if (!sendCondition()) {
                            // A half-closed request stream cannot grant further page credit.
                            if (this.halfClosed.get() && !this.packages.isEmpty()) {
                                terminate(Status.FAILED_PRECONDITION.withDescription(
                                        "Partition scan needs more response credit after client half-close")
                                                                   .asRuntimeException());
                            }
                            return;
                        }
                        ScanResponse response = this.packages.poll(10, TimeUnit.MILLISECONDS);
                        if (response != null) {
                            this.sender.onNext(response.toBuilder().setSeqNo(this.nextSeqNo.get()).build());
                            this.nextSeqNo.incrementAndGet();
                            startRead();
                        } else if (this.readOver.get() && this.packages.isEmpty()) {
                            terminate(null);
                            return;
                        } else {
                            return;
                        }
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    terminate(failure);
                } catch (RuntimeException | Error failure) {
                    terminate(failure);
                } finally {
                    this.sending.set(false);
                    if (sendWorkAvailable() && (!this.packages.isEmpty() || this.readOver.get())) {
                        startSend();
                    }
                }
            });
        } catch (RuntimeException | Error failure) {
            this.sending.set(false);
            terminate(failure);
        }
    }

    private void closeIterator() {
        synchronized (this.iteratorLock) {
            if (this.iter == null || this.iteratorClosed) {
                return;
            }
            this.iteratorClosed = true;
            try {
                this.iter.close();
            } catch (RuntimeException | Error failure) {
                this.failedCleanup.accept(failure);
                throw failure;
            }
        }
    }

    private void terminate(Throwable failure) {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        this.readOver.set(true);
        this.packages.clear();
        if (failure != null) {
            synchronized (this.readerLock) {
                // Keep ownership protected until interrupt is issued; the pool may reuse this thread.
                if (this.reader != null && this.reader != Thread.currentThread()) {
                    this.reader.interrupt();
                }
            }
        }
        try {
            closeIterator();
        } catch (RuntimeException | Error cleanup) {
            if (failure == null) {
                failure = cleanup;
            } else if (failure != cleanup) {
                failure.addSuppressed(cleanup);
            }
        }
        if (failure == null) {
            this.sender.onCompleted();
        } else {
            this.sender.onError(failure);
        }
    }
}
