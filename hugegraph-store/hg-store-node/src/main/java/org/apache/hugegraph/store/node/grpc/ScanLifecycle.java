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

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** Per-call response and cleanup accounting; a failed release remains registered. */
final class ScanLifecycle {

    // 0 = open, 1 = cancellation won, 2 = response terminated.
    private final AtomicInteger terminal = new AtomicInteger();
    private final Object responseLock = new Object();
    private int activities;
    private volatile Throwable cleanupFailure;
    private Runnable finished = () -> { };

    synchronized void onFinished(Runnable action) {
        this.finished = action;
    }

    synchronized void enter() {
        this.activities++;
    }

    synchronized boolean tryEnter() {
        if (this.terminal.get() != 0) {
            return false;
        }
        this.activities++;
        return true;
    }

    synchronized boolean tryEnterCancellation() {
        if (!tryCancel()) {
            return false;
        }
        this.activities++;
        return true;
    }

    synchronized void leave() {
        this.activities--;
        checkFinished();
    }

    synchronized void failedCleanup(Throwable failure) {
        if (this.cleanupFailure == null) {
            this.cleanupFailure = failure;
        }
    }

    Throwable cleanupFailure() {
        return this.cleanupFailure;
    }

    private synchronized void checkFinished() {
        if (this.terminal.get() == 2 && this.activities == 0 && this.cleanupFailure == null) {
            this.finished.run();
        }
    }

    boolean tryCancel() {
        return this.terminal.compareAndSet(0, 1);
    }

    boolean isCancelled() {
        return this.terminal.get() == 1;
    }

    void finishWithoutResponse() {
        this.terminal.set(2);
        checkFinished();
    }

    boolean close(ScanIterator iterator) {
        if (iterator == null) {
            return true;
        }
        try {
            iterator.close();
            return true;
        } catch (RuntimeException | Error failure) {
            failedCleanup(failure);
            return false;
        }
    }

    void execute(ThreadPoolExecutor executor, Runnable task) {
        enter();
        AtomicBoolean released = new AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) {
                leave();
            }
        };
        try {
            executor.execute(() -> {
                try {
                    task.run();
                } finally {
                    release.run();
                }
            });
        } catch (RuntimeException | Error failure) {
            release.run();
            if (failure instanceof RejectedExecutionException) {
                throw rejected(executor, failure);
            }
            throw failure;
        }
    }

    static RuntimeException rejected(ThreadPoolExecutor executor, Throwable failure) {
        Status status = executor.isShutdown() ? Status.UNAVAILABLE : Status.RESOURCE_EXHAUSTED;
        return status.withDescription("Store scan task rejected").withCause(failure)
                     .asRuntimeException();
    }

    <T> StreamObserver<T> response(StreamObserver<T> delegate) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(T value) {
                enter();
                try {
                    synchronized (responseLock) {
                        if (terminal.get() == 0) {
                            delegate.onNext(value);
                        }
                    }
                } finally {
                    leave();
                }
            }

            @Override
            public void onError(Throwable failure) {
                enter();
                try {
                    synchronized (responseLock) {
                        if (terminal.getAndSet(2) != 2) {
                            delegate.onError(failure);
                        }
                    }
                } finally {
                    leave();
                }
            }

            @Override
            public void onCompleted() {
                enter();
                try {
                    synchronized (responseLock) {
                        int previous = terminal.getAndSet(2);
                        if (previous == 0) {
                            delegate.onCompleted();
                        } else if (previous == 1) {
                            delegate.onError(Status.CANCELLED
                                                   .withDescription("Store scan cancelled before completion")
                                                   .asRuntimeException());
                        }
                    }
                } finally {
                    leave();
                }
            }
        };
    }
}
