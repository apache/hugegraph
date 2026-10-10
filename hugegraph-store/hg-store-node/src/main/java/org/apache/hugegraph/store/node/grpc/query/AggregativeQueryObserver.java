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

import static org.apache.hugegraph.store.node.grpc.query.AggregativeQueryService.errorResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.store.business.MultiPartitionIterator;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.query.QueryRequest;
import org.apache.hugegraph.store.grpc.query.QueryResponse;
import org.apache.hugegraph.store.node.grpc.query.model.PipelineResult;
import org.apache.hugegraph.store.node.grpc.query.model.QueryPlan;
import org.apache.hugegraph.store.node.grpc.query.stages.EarlyStopException;
import org.apache.hugegraph.store.query.KvSerializer;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;

import com.google.protobuf.ByteString;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AggregativeQueryObserver implements StreamObserver<QueryRequest> {

    private static final int RESULT_COUNT = 16;
    private final ExecutorService threadPool;
    private final long timeout;
    private final int batchSize;
    private final AtomicInteger consumeCount = new AtomicInteger(0);
    private final AtomicInteger sendCount = new AtomicInteger(0);
    private final AtomicBoolean clientCanceled = new AtomicBoolean(false);
    private final BinaryElementSerializer serializer = BinaryElementSerializer.getInstance();
    private final StreamObserver<QueryResponse> sender;
    private volatile ScanIterator iterator = null;
    private QueryPlan plan = null;
    private String queryId;
    private final Consumer<AggregativeQueryObserver> completion;
    private final Set<Thread> workers = new HashSet<>();
    private int pendingTasks;
    private boolean finished;
    private volatile boolean requestCompleted;
    private final Object responseLock = new Object();
    private boolean responseFinished;
    private boolean completeResponse = true;
    private QueryResponse finalResponse;
    private boolean errorReported;
    private final AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();

    public AggregativeQueryObserver(StreamObserver<QueryResponse> sender,
                                    ExecutorService threadPool, long timeout,
                                    int batchSize) {
        this(sender, threadPool, timeout, batchSize, ignored -> { });
    }

    AggregativeQueryObserver(StreamObserver<QueryResponse> sender,
                             ExecutorService threadPool, long timeout, int batchSize,
                             Consumer<AggregativeQueryObserver> completion) {
        this.sender = sender;
        this.threadPool = threadPool;
        this.batchSize = batchSize;
        this.timeout = timeout;
        this.completion = completion;
    }

    ScanIterator getIterator(QueryRequest request) {
        return QueryUtil.getIterator(request);
    }

    ScanIterator getCountIterator(QueryRequest request) {
        return new QueryUtil().getHandler().scanAll(request.getGraph(), request.getTable());
    }

    void runUnary(QueryRequest request, boolean countOnly) {
        synchronized (this) {
            if (this.finished || this.clientCanceled.get()) {
                return;
            }
            this.queryId = request.getQueryId();
            this.pendingTasks = 1;
            this.workers.add(Thread.currentThread());
        }
        Context context = Context.current();
        Context.CancellationListener cancellation = ignored -> onTransportCancel();
        Throwable failure = null;
        try {
            // Unary callbacks occupy the call executor while reading. Context cancellation
            // must interrupt that read without waiting for the queued onCancel callback.
            context.addListener(cancellation, Runnable::run);
            if (this.clientCanceled.get()) {
                return;
            }
            this.iterator = countOnly ? getCountIterator(request) : getIterator(request);
            QueryResponse.Builder response = getBuilder();
            Kv.Builder kv = getKvBuilder();
            long count = countOnly ? countPartitions() : 0;
            while (!countOnly && !this.clientCanceled.get() && this.iterator.hasNext()) {
                Object next = this.iterator.next();
                if (next != null) {
                    RocksDBSession.BackendColumn column = (RocksDBSession.BackendColumn) next;
                    response.addData(kv.setKey(ByteString.copyFrom(column.name))
                                       .setValue(column.value == null ? ByteString.EMPTY :
                                                 ByteString.copyFrom(column.value)).build());
                }
            }
            if (countOnly) {
                List<Object> counts = new ArrayList<>();
                for (int i = 0; i < request.getFunctionsCount(); i++) {
                    counts.add(new AtomicLong(count));
                }
                response.addData(kv.setKey(ByteString.copyFrom(KvSerializer.toBytes(List.of())))
                                   .setValue(ByteString.copyFrom(KvSerializer.toBytes(counts))).build());
            }
            synchronized (this.responseLock) {
                this.finalResponse = response.setQueryId(this.queryId).setIsOk(true)
                                             .setIsFinished(true).build();
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            synchronized (this.responseLock) {
                this.finalResponse = errorResponse(getBuilder(), this.queryId, e);
            }
        } finally {
            try {
                context.removeListener(cancellation);
            } finally {
                workerFinished(failure);
            }
        }
    }

    private long countIterator(ScanIterator source) {
        long count = 0;
        while (!this.clientCanceled.get() && source.hasNext()) {
            source.next();
            count++;
        }
        return count;
    }

    private long countPartitions() {
        if (!(this.iterator instanceof MultiPartitionIterator)) {
            return countIterator(this.iterator);
        }
        List<ScanIterator> children = ((MultiPartitionIterator) this.iterator).getIterators();
        CountDownLatch completed = new CountDownLatch(children.size());
        AtomicLong total = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        synchronized (this) {
            this.pendingTasks += children.size();
        }
        int submitted = 0;
        try {
            for (ScanIterator child : children) {
                this.threadPool.execute(() -> {
                    taskStarted();
                    try {
                        total.addAndGet(countIterator(child));
                    } catch (RuntimeException | Error e) {
                        recordCountFailure(failure, e);
                    } finally {
                        try {
                            cleanup(child::close);
                        } finally {
                            workerFinished();
                            completed.countDown();
                        }
                    }
                });
                submitted++;
            }
        } catch (RuntimeException | Error e) {
            recordCountFailure(failure, e);
            for (int i = submitted; i < children.size(); i++) {
                ScanIterator child = children.get(i);
                try {
                    cleanup(child::close);
                } finally {
                    taskFinished();
                    completed.countDown();
                }
            }
        }
        boolean interrupted = false;
        for (;;) {
            try {
                completed.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        Throwable error = failure.get();
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
        }
        if (error instanceof Error) {
            throw (Error) error;
        }
        return total.get();
    }

    private static void recordCountFailure(AtomicReference<Throwable> failure, Throwable error) {
        synchronized (failure) {
            if (failure.get() == null) {
                failure.set(error);
            } else if (failure.get() != error) {
                failure.get().addSuppressed(error);
            }
        }
    }

    QueryPlan buildPlan(QueryRequest request) {
        return QueryUtil.buildPlan(request);
    }

    @Override
    public synchronized void onNext(QueryRequest request) {
        if (this.clientCanceled.get() || this.finished || this.requestCompleted) {
            return;
        }
        if (this.queryId == null) {
            log.debug("got request: {}", request);
            this.queryId = request.getQueryId();
        }

        // the first request, start the sending thread
        if (iterator == null) {
            long current = System.nanoTime();
            this.pendingTasks = 1;
            try {
                iterator = getIterator(request);
                plan = buildPlan(request);
                threadPool.execute(this::sendData);
            } catch (RuntimeException | Error e) {
                // The framework must report the original synchronous failure, not normal completion.
                synchronized (this.responseLock) {
                    this.completeResponse = false;
                }
                taskFinished();
                Throwable failure = this.cleanupFailure.get();
                if (failure != null && failure != e) {
                    e.addSuppressed(failure);
                }
                throw e;
            }
            log.debug("query id: {}, init data cost: {} ms", queryId,
                      (System.nanoTime() - current) * 1.0 / 1000000);
        } else {
            this.consumeCount.incrementAndGet();
            log.debug("query id: {}, send feedback of {}", queryId, this.consumeCount.get());
        }
    }

    @Override
    public void onError(Throwable t) {
        onTransportCancel();
        log.error("AggregativeQueryService, query id: {},  got error", this.queryId, t);
    }

    void onTransportCancel() {
        // Both transport cancellation and request onError can report the same event.
        synchronized (this.responseLock) {
            if (this.responseFinished) {
                return;
            }
            this.completeResponse = false;
            this.responseFinished = true;
        }
        cancel();
    }

    @Override
    public synchronized void onCompleted() {
        // Half-close ends feedback, not work already supported by its existing credit.
        this.requestCompleted = true;
        if (this.pendingTasks == 0 && !this.finished) {
            this.finished = true;
            finishResponse();
        }
    }

    void cancelForShutdown() {
        try {
            synchronized (this.responseLock) {
                if (!this.responseFinished) {
                    this.responseFinished = true;
                    this.completeResponse = false;
                    this.sender.onError(Status.UNAVAILABLE.withDescription("Store queries are stopping")
                                                          .asRuntimeException());
                }
            }
        } finally {
            cancel();
        }
    }

    public void cancel() {
        this.clientCanceled.set(true);
        synchronized (this) {
            for (Thread worker : this.workers) {
                worker.interrupt();
            }
            // An RPC with no first request has no iterator or cleanup task.
            if (this.pendingTasks == 0 && !this.finished) {
                this.finished = true;
                finishResponse();
            }
        }
    }

    private synchronized void taskStarted() {
        this.workers.add(Thread.currentThread());
    }

    private void workerFinished() {
        workerFinished(null);
    }

    private void workerFinished(Throwable failure) {
        synchronized (this) {
            this.workers.remove(Thread.currentThread());
        }
        taskFinished(failure);
    }

    private void taskFinished() {
        taskFinished(null);
    }

    private void taskFinished(Throwable primaryFailure) {
        boolean cleanup;
        synchronized (this) {
            cleanup = --this.pendingTasks == 0;
            if (cleanup) {
                this.finished = true;
            }
        }
        if (cleanup) {
            if (this.plan != null) {
                cleanup(this.plan::clear);
            }
            if (this.iterator != null) {
                cleanup(this.iterator::close);
            }
            Throwable releaseFailure = this.cleanupFailure.get();
            if (primaryFailure != null && releaseFailure != null && primaryFailure != releaseFailure) {
                primaryFailure.addSuppressed(releaseFailure);
            }
            finishResponse();
        }
    }

    private void cleanup(Runnable release) {
        try {
            release.run();
        } catch (RuntimeException | Error failure) {
            synchronized (this.cleanupFailure) {
                Throwable first = this.cleanupFailure.get();
                if (first == null) {
                    this.cleanupFailure.set(failure);
                } else if (first != failure) {
                    first.addSuppressed(failure);
                }
            }
            log.error("Aggregate query {} failed to release resources; Store shutdown remains blocked",
                      this.queryId, failure);
        }
    }

    private void finishResponse() {
        try {
            synchronized (this.responseLock) {
                if (this.completeResponse && !this.responseFinished) {
                    this.responseFinished = true;
                    Throwable failure = this.cleanupFailure.get();
                    if (this.finalResponse != null && !this.finalResponse.getIsOk() &&
                        !this.clientCanceled.get()) {
                        this.sender.onNext(this.finalResponse);
                    } else if (failure != null && !this.errorReported) {
                        this.sender.onNext(errorResponse(getBuilder(), this.queryId, failure));
                    } else if (failure == null && this.finalResponse != null &&
                               !this.clientCanceled.get()) {
                        this.sender.onNext(this.finalResponse);
                    }
                    this.sender.onCompleted();
                }
            }
        } finally {
            // A terminal RPC does not prove that its native resources were released.
            if (this.cleanupFailure.get() == null) {
                this.completion.accept(this);
            }
        }
    }

    private void sendResponse(QueryResponse response) {
        synchronized (this.responseLock) {
            if (!this.responseFinished && !this.clientCanceled.get()) {
                if (response.getIsOk() && response.getIsFinished()) {
                    // The client treats this batch as success; publish it only after cleanup.
                    this.finalResponse = response;
                } else {
                    this.errorReported |= !response.getIsOk();
                    this.sender.onNext(response);
                }
            }
        }
    }

    public void sendData() {
        taskStarted();
        try {
            long lastSend = System.currentTimeMillis();
            var responseBuilder = getBuilder();
            var kvBuilder = getKvBuilder();

            while (!this.clientCanceled.get()) {
                // produces more result than consumer, just waiting
                if (sendCount.get() - consumeCount.get() >= RESULT_COUNT) {
                    if (this.requestCompleted) {
                        sendResponse(errorResponse(getBuilder(), queryId,
                                                   new IllegalStateException(
                                                           "Request completed without enough feedback")));
                        cancel();
                        return;
                    }
                    // read timeout, takes long time not to read data
                    if (System.currentTimeMillis() - lastSend > timeout) {
                        sendResponse(errorResponse(getBuilder(), queryId,
                                                   new RuntimeException("sending-timeout, server closed")));
                        cancel();
                        return;
                    }

                    try {
                        Thread.sleep(1000);
                        continue;
                    } catch (InterruptedException ignore) {
                        log.warn("send data is interrupted, {}", ignore.getMessage());
                    }
                }

                var builder = readBatchData(responseBuilder, kvBuilder);
                if (builder == null || this.clientCanceled.get()) {
                    break;
                } else {
                    try {
                        builder.setQueryId(queryId);
                        sendResponse(builder.build());
                        this.sendCount.incrementAndGet();
                        lastSend = System.currentTimeMillis();
                    } catch (Exception e) {
                        log.error("send data got error: ", e);
                        cancel();
                        break;
                    }
                }

                if (!builder.getIsOk()) {
                    // Report the internal error before cancelling remaining partition work.
                    cancel();
                    break;
                }
                if (builder.getIsFinished()) {
                    break;
                }
            }
        } catch (Exception e) {
            try {
                sendResponse(errorResponse(getBuilder(), queryId, e));
            } finally {
                cancel();
            }
        } finally {
            workerFinished();
        }
    }

    /**
     * 1.1: pipeline is empty:
     * --> read data from iterator
     * 1.2: pipeline is not empty
     * 1.2.1: only stop stage: --> just finish
     * 1.2.2: has Agg or top or sort --> multi thread
     * 1.2.3: plain stage: --> read data from iterator through pipeline
     *
     * @return result builder
     */
    private QueryResponse.Builder readBatchData(QueryResponse.Builder builder,
                                                Kv.Builder kvBuilder) {
        ScanIterator itr = this.iterator;
        boolean empty = plan.isEmpty();
        boolean finish = false;
        boolean checkIterator = true;

        int count = 0;
        long current = System.nanoTime();

        try {
            if (!empty) {
                if (this.plan.onlyStopStage()) {
                    builder.setIsOk(true).setIsFinished(true);
                    return builder;
                } else if (this.plan.hasIteratorResult()) {
                    checkIterator = false;
                    AtomicReference<Throwable> exception = new AtomicReference<>();
                    if (this.iterator instanceof MultiPartitionIterator) {
                        var iterators = ((MultiPartitionIterator) this.iterator).getIterators();
                        CountDownLatch latch = new CountDownLatch(iterators.size());
                        synchronized (this) {
                            this.pendingTasks += iterators.size();
                        }
                        int submitted = 0;
                        try {
                            for (var itr2 : iterators) {
                                threadPool.execute(() -> {
                                    taskStarted();
                                    try {
                                        execute(itr2);
                                    } catch (RuntimeException | Error e) {
                                        exception.compareAndSet(null, e);
                                    } finally {
                                        try {
                                            cleanup(itr2::close);
                                            Throwable failure = this.cleanupFailure.get();
                                            if (failure != null) {
                                                exception.compareAndSet(null, new RuntimeException(
                                                        "partition iterator cleanup failed", failure));
                                            }
                                        } finally {
                                            latch.countDown();
                                            workerFinished();
                                        }
                                    }
                                });
                                submitted++;
                            }
                        } catch (RuntimeException | Error failure) {
                            // Rejection must also release iterators whose tasks were not accepted.
                            for (int i = submitted; i < iterators.size(); i++) {
                                try {
                                    cleanup(iterators.get(i)::close);
                                } finally {
                                    latch.countDown();
                                    taskFinished();
                                }
                            }
                            throw failure;
                        }
                        if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                            throw new TimeoutException("partition query timed out");
                        }
                        if (exception.get() != null) {
                            throw new RuntimeException("partition query failed", exception.get());
                        }
                    } else {
                        // can't be parallel, but has agg like stage
                        execute(this.iterator);
                    }

                    if (this.clientCanceled.get()) {
                        return builder.setIsOk(false).setIsFinished(false);
                    }
                    try {
                        // last empty element
                        itr = (ScanIterator) plan.execute(PipelineResult.EMPTY);
                    } catch (EarlyStopException ignore) {
                    }
                } else {
                    itr = executePlainPipeline(this.iterator);
                }
            }

            builder.clear();

            List<Kv> batchResult = new ArrayList<>();
            while (!this.clientCanceled.get() && itr.hasNext()) {
                if (count >= batchSize) {
                    break;
                }

                if (empty) {
                    // reading from raw iterator
                    var column = (RocksDBSession.BackendColumn) iterator.next();
                    if (column != null) {
                        batchResult.add(kvBuilder.clear().setKey(ByteString.copyFrom(column.name))
                                                 .setValue(column.value == null ? ByteString.EMPTY :
                                                           ByteString.copyFrom(column.value))
                                                 .build());
                        // builder.addData(kvBuilder.setKey(ByteString.copyFrom(column.name))
                        //        .setValue(column.value == null ? ByteString.EMPTY : ByteString
                        //        .copyFrom(column.value))
                        //        .build());
                        count++;
                    }
                } else {
                    // pass through pipeline
                    PipelineResult result = itr.next();
                    if (result == null) {
                        continue;
                    }

                    if (result == PipelineResult.EMPTY) {
                        finish = true;
                        break;
                    }
                    count++;
                    batchResult.add(toKv(kvBuilder, result));
                    // builder.addData(toKv(result));
                }
            }

            builder.addAllData(batchResult);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("readBatchData got error: ", e);
            return builder.setIsOk(false).setIsFinished(false).setMessage("Store Server Error: "
                                                                          + Arrays.toString(
                    e.getStackTrace()));
        }

        if (checkIterator) {
            // check the iterator
            finish = this.clientCanceled.get() || !itr.hasNext();
        }
        log.debug("query id: {}, finished batch, with size :{}, finish:{}, cost: {} ms", queryId,
                  count,
                  finish, (System.nanoTime() - current) * 1.0 / 1000000);

        return builder.setIsOk(true).setIsFinished(finish);
    }

    public ScanIterator executePlainPipeline(ScanIterator itr) {
        return new ScanIterator() {
            private boolean limitFlag = false;

            @Override
            public boolean hasNext() {
                return itr.hasNext() && !limitFlag;
            }

            @Override
            public boolean isValid() {
                return itr.isValid();
            }

            @Override
            public <T> T next() {
                try {
                    return (T) executePipeline(itr.next());
                } catch (EarlyStopException ignore) {
                    limitFlag = true;
                    return (T) PipelineResult.EMPTY;
                }
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Used for parallelized  process
     *
     * @param itr input iterator
     */
    private void execute(ScanIterator itr) {
        long recordCount = 0;
        long current = System.nanoTime();
        while (!this.clientCanceled.get() && itr.hasNext()) {
            try {
                recordCount++;
                executePipeline(itr.next());
                if (System.nanoTime() - current > timeout * 1_000_000) {
                    throw new RuntimeException("execution timeout");
                }
            } catch (EarlyStopException ignore) {
                // The limit stage will throw an exception to abort the execution early
                // log.warn("query id: {}, early stop: {}", this.queryId, e.getMessage());
                break;
            }
        }
        log.debug("query id: {}, read records: {}", this.queryId, recordCount);
    }

    private Object executePipeline(Object obj) throws EarlyStopException {
        PipelineResult input;
        if (obj instanceof RocksDBSession.BackendColumn) {
            input = new PipelineResult((RocksDBSession.BackendColumn) obj);
        } else if (obj instanceof BaseElement) {
            input = new PipelineResult((BaseElement) obj);
        } else {
            return null;
        }

        return plan.execute(input);
    }

    private QueryResponse.Builder getBuilder() {
        return QueryResponse.newBuilder();
        // return localBuilder.get().clear();
    }

    private Kv.Builder getKvBuilder() {
        return Kv.newBuilder();
        // return localKvBuilder.get().clear();
    }

    private Kv toKv(Kv.Builder builder, PipelineResult result) {
        builder.clear();
        switch (result.getResultType()) {
            case BACKEND_COLUMN:
                var column = result.getColumn();
                builder.setKey(ByteString.copyFrom(column.name));
                builder.setValue(column.value == null ? ByteString.EMPTY :
                                 ByteString.copyFrom(column.value));
                break;
            case MKV:
                var mkv = result.getKv();
                builder.setKey(ByteString.copyFrom(KvSerializer.toBytes(mkv.getKeys())));
                builder.setValue(ByteString.copyFrom(KvSerializer.toBytes(mkv.getValues())));
                break;
            case HG_ELEMENT:
                var element = result.getElement();
                // builder.setKey(ByteString.copyFrom(element.id().asBytes()));
                BackendColumn backendColumn;
                if (element instanceof BaseVertex) {
                    backendColumn = serializer.writeVertex((BaseVertex) element);
                } else { // if (element instanceof BaseEdge) {
                    backendColumn = serializer.writeEdge((BaseEdge) element);
                }

                builder.setKey(ByteString.copyFrom(backendColumn.name));
                builder.setValue(ByteString.copyFrom(backendColumn.value));

                break;
            default:
                throw new RuntimeException("unsupported result type: " + result.getResultType());
        }

        return builder.build();
    }
}
