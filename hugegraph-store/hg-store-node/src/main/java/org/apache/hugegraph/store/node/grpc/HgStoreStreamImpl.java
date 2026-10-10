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

import java.util.Map;
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.grpc.Graphpb.ScanPartitionRequest;
import org.apache.hugegraph.store.grpc.Graphpb.ScanResponse;
import org.apache.hugegraph.store.node.grpc.scan.ScanResponseObserver;
import org.apache.hugegraph.store.grpc.state.ScanState;
import org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc;
import org.apache.hugegraph.store.grpc.stream.KvPageRes;
import org.apache.hugegraph.store.grpc.stream.KvStream;
import org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq;
import org.apache.hugegraph.store.grpc.stream.ScanStreamReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.util.HgExecutorUtil;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/10/19
 */
@Slf4j
@GRpcService
public class HgStoreStreamImpl extends HgStoreStreamGrpc.HgStoreStreamImplBase {

    @Autowired
    private HgStoreNodeService storeService;
    @Autowired
    private AppConfig appConfig;
    private HgStoreWrapperEx wrapper;
    private ThreadPoolExecutor executor;
    private boolean closing;
    private final Map<ScanLifecycle, Runnable> scans = new ConcurrentHashMap<>();

    /** Close admission before cancelling the gRPC server. */
    public synchronized void stopAcceptingScans() {
        this.closing = true;
    }

    /** Cancel streams, then drain queued cleanup tasks. Never discard the queue. */
    public void shutdownScans() {
        stopAcceptingScans();
        for (Runnable cancel : this.scans.values().toArray(new Runnable[0])) {
            try {
                cancel.run();
            } catch (RuntimeException | Error failure) {
                log.warn("Failed to cancel scan; continuing other cancellations", failure);
            }
        }
        ThreadPoolExecutor current = getRealExecutor();
        if (current != null) {
            current.shutdown();
        }
    }

    private synchronized void checkAcceptingScans() {
        if (this.closing) {
            throw Status.UNAVAILABLE.withDescription("Store scans are stopping")
                                    .asRuntimeException();
        }
    }

    /** Wait independently of executor termination: failed native release is sticky. */
    public void awaitScanCleanup() {
        boolean interrupted = false;
        long nextLog = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        synchronized (this.scans) {
            while (!this.scans.isEmpty()) {
                try {
                    this.scans.wait(5000L);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
                if (!this.scans.isEmpty() && System.nanoTime() - nextLog >= 0) {
                    log.warn("Still waiting for {} scans to clean up before closing databases",
                             this.scans.size());
                    for (ScanLifecycle scan : this.scans.keySet()) {
                        Throwable failure = scan.cleanupFailure();
                        if (failure != null) {
                            log.warn("Scan cleanup failed; database close stays blocked", failure);
                        }
                    }
                    nextLog = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private <T> StreamObserver<T> register(
            ScanLifecycle lifecycle, Function<ScanLifecycle, StreamObserver<T>> factory) {
        Context context = Context.current();
        StreamObserver<T> observer = factory.apply(lifecycle);
        Runnable cancel = () -> {
            if (lifecycle.tryEnterCancellation()) {
                try {
                    observer.onError(Status.CANCELLED.asRuntimeException());
                } finally {
                    lifecycle.leave();
                }
            }
        };
        this.scans.put(lifecycle, cancel);
        Context.CancellationListener listener = ignored -> cancel.run();
        lifecycle.onFinished(() -> {
            this.scans.remove(lifecycle);
            context.removeListener(listener);
            synchronized (this.scans) {
                this.scans.notifyAll();
            }
        });
        context.addListener(listener, Runnable::run);
        return new StreamObserver<T>() {
            @Override
            public void onNext(T value) {
                if (lifecycle.tryEnter()) {
                    try {
                        observer.onNext(value);
                    } finally {
                        lifecycle.leave();
                    }
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (lifecycle.tryEnterCancellation()) {
                    try {
                        observer.onError(failure);
                    } finally {
                        lifecycle.leave();
                    }
                }
            }

            @Override
            public void onCompleted() {
                if (lifecycle.tryEnter()) {
                    try {
                        observer.onCompleted();
                    } finally {
                        lifecycle.leave();
                    }
                }
            }
        };
    }

    public StreamObserver<ScanPartitionRequest> scanGraphPartition(
            StreamObserver<ScanResponse> response,
            BusinessHandler handler) {
        synchronized (this) {
            checkAcceptingScans();
            return register(new ScanLifecycle(), lifecycle ->
                    new ScanResponseObserver<>(
                            lifecycle.response(response), handler,
                            task -> lifecycle.execute(getExecutor(), task), lifecycle::failedCleanup));
        }
    }

    private <T> void oneShot(StreamObserver<T> response, Consumer<ScanLifecycle> action) {
        ScanLifecycle lifecycle = new ScanLifecycle();
        Thread caller = Thread.currentThread();
        boolean alreadyInterrupted = caller.isInterrupted();
        AtomicBoolean interruptedByCancellation = new AtomicBoolean();
        Context context = Context.current();
        Runnable cancel = () -> {
            synchronized (lifecycle) {
                if (lifecycle.tryCancel()) {
                    // Wake interruptible reads; native reads retain ownership until they return.
                    if (!caller.isInterrupted()) {
                        interruptedByCancellation.set(true);
                        caller.interrupt();
                    }
                }
            }
        };
        Context.CancellationListener listener = ignored -> cancel.run();
        synchronized (this) {
            checkAcceptingScans();
            lifecycle.enter();
            this.scans.put(lifecycle, cancel);
            lifecycle.onFinished(() -> {
                this.scans.remove(lifecycle);
                synchronized (this.scans) {
                    this.scans.notifyAll();
                }
            });
        }
        try {
            context.addListener(listener, Runnable::run);
            action.accept(lifecycle);
        } finally {
            try {
                // An iterator may close automatically during hasNext(), after which the
                // scan exits without entering its ordinary response-completion path.
                if (lifecycle.isCancelled()) {
                    lifecycle.response(response).onError(Status.CANCELLED
                            .withDescription("Store scan cancelled before completion")
                            .asRuntimeException());
                }
            } finally {
                context.removeListener(listener);
                // A cancellation snapshot must not interrupt this executor thread after reuse.
                synchronized (lifecycle) {
                    lifecycle.finishWithoutResponse();
                    lifecycle.leave();
                    if (!alreadyInterrupted && interruptedByCancellation.get()) {
                        Thread.interrupted();
                    }
                }
            }
        }
    }

    private HgStoreWrapperEx getWrapper() {
        if (this.wrapper == null) {
            synchronized (this) {
                if (this.wrapper == null) {
                    this.wrapper = new HgStoreWrapperEx(
                            storeService.getStoreEngine().getBusinessHandler());
                }
            }
        }
        return this.wrapper;
    }

    public synchronized ThreadPoolExecutor getRealExecutor() {
        return this.executor;
    }

    public synchronized ThreadPoolExecutor getExecutor() {
        checkAcceptingScans();
        if (this.executor == null) {
            AppConfig.ThreadPoolScan scan = this.appConfig.getThreadPoolScan();
            this.executor = HgExecutorUtil.createExecutor("hg-scan", scan.getCore(),
                                                         scan.getMax(), scan.getQueue());
        }
        return this.executor;
    }

    public ScanState getState() {
        ThreadPoolExecutor ex = getExecutor();
        ScanState.Builder builder = ScanState.newBuilder();
        BlockingQueue<Runnable> queue = ex.getQueue();
        ScanState state =
                builder.setActiveCount(ex.getActiveCount()).setTaskCount(ex.getTaskCount())
                       .setCompletedTaskCount(ex.getCompletedTaskCount())
                       .setMaximumPoolSize(ex.getMaximumPoolSize())
                       .setLargestPoolSize(ex.getLargestPoolSize()).setPoolSize(ex.getPoolSize())
                       .setAddress(appConfig.getStoreServerAddress())
                       .setQueueSize(queue.size())
                       .setQueueRemainingCapacity(queue.remainingCapacity())
                       .build();
        return state;
    }

    @Override
    public synchronized StreamObserver<ScanStreamReq> scan(StreamObserver<KvPageRes> response) {
        checkAcceptingScans();
        return register(new ScanLifecycle(), lifecycle ->
                new ScanStreamResponse(response, getWrapper(), getExecutor(), appConfig, lifecycle));
    }

    @Override
    public void scanOneShot(ScanStreamReq request, StreamObserver<KvPageRes> response) {
        oneShot(response, lifecycle -> ScanOneShotResponse.scanOneShot(request, response, getWrapper(), lifecycle));
    }

    @Override
    public synchronized StreamObserver<ScanStreamBatchReq> scanBatch(StreamObserver<KvPageRes> response) {
        checkAcceptingScans();
        return register(new ScanLifecycle(), lifecycle ->
                ScanBatchResponse3.of(response, getWrapper(), getExecutor(), lifecycle));
    }

    @Override
    public StreamObserver<ScanStreamBatchReq> scanBatch2(StreamObserver<KvStream> response) {
        StreamObserver<ScanStreamBatchReq> observer;
        synchronized (this) {
            checkAcceptingScans();
            observer = register(new ScanLifecycle(), lifecycle ->
                    ScanBatchResponseFactory.of(response, getWrapper(), getExecutor(), lifecycle));
        }
        ScanBatchResponseFactory.getInstance().checkStreamActive();
        return observer;
    }

    @Override
    public void scanBatchOneShot(ScanStreamBatchReq request, StreamObserver<KvPageRes> response) {
        oneShot(response, lifecycle ->
                ScanBatchOneShotResponse.scanOneShot(request, response, getWrapper(), lifecycle));
    }
}
