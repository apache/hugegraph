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

package org.apache.hugegraph.store.client.query;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hugegraph.store.client.type.HgStoreClientException;

import io.grpc.stub.StreamObserver;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommonKvStreamObserver<R, T> implements StreamObserver<R> {

    /**
     * Queue to store result
     */
    private final BlockingQueue<Iterator<T>> queue;

    /**
     * Function that send requests to server
     */
    @Setter
    private Consumer<Boolean> requestSender;

    /**
     * Close the request stream: true means serialized half-close; false means transport cancellation.
     */
    @Setter
    private Consumer<Boolean> transferComplete;

    /**
     * Parser for data returned from server
     */
    private final Function<R, Iterator<T>> valueExtractor;
    /**
     * Parser for data state returned from server
     */
    private final Function<R, ResultState> stateWatcher;

    private final Object stateLock = new Object();
    // Only outgoing sends and normal half-close share this lock. Cancellation bypasses it.
    private final Object requestLock = new Object();
    private volatile ResultState terminal;
    private volatile int parsing;
    private volatile boolean closed;
    private volatile long current = System.nanoTime();
    private String queryId;

    @Setter
    private long timeout = 1800 * 1000;

    public CommonKvStreamObserver(Function<R, Iterator<T>> valueExtractor,
                                  Function<R, ResultState> stateWatcher) {
        this.queue = new LinkedBlockingQueue<>();
        this.valueExtractor = valueExtractor;
        this.stateWatcher = stateWatcher;
    }

    /**
     * Send requests
     */
    public void sendRequest() {
        synchronized (this.requestLock) {
            synchronized (this.stateLock) {
                if (this.closed || this.terminal != null) {
                    return;
                }
                this.current = System.nanoTime();
            }
            this.requestSender.accept(true);
        }
    }

    public boolean isServerFinished() {
        ResultState state = this.terminal;
        return this.closed || state == ResultState.ERROR ||
               (state == ResultState.FINISHED && this.parsing == 0);
    }

    @Override
    public void onNext(R value) {
        synchronized (this.stateLock) {
            if (this.closed || this.terminal != null) {
                return;
            }
            this.parsing++;
            this.current = System.nanoTime();
        }

        Iterator<T> iterator = null;
        ResultState result = ResultState.ERROR;
        try {
            ResultState responseState = this.stateWatcher.apply(value);
            if (responseState == ResultState.IDLE || responseState == ResultState.FINISHED) {
                iterator = Objects.requireNonNull(this.valueExtractor.apply(value), "response iterator");
                result = responseState;
            } else {
                iterator = new ErrorMessageIterator<>(responseState.getMessage());
                result = ResultState.ERROR;
            }
        } catch (Throwable e) {
            log.error("handling server data for query {}, got error: ", this.queryId, e);
            result = ResultState.ERROR;
            iterator = new ErrorMessageIterator<>(e.getMessage());
            if (e instanceof Error) {
                throw (Error) e;
            }
        } finally {
            synchronized (this.stateLock) {
                // Completion can arrive while parsing. Publish before releasing the parsing count.
                // An error or client close instead discards responses still being parsed.
                if (!this.closed && this.terminal != ResultState.ERROR) {
                    if (iterator != null) {
                        this.queue.offer(iterator);
                    }
                    // A failed accepted response overrides completion received during parsing.
                    if (result == ResultState.ERROR ||
                        (this.terminal == null && result != ResultState.IDLE)) {
                        this.terminal = result;
                    }
                }
                this.parsing--;
                this.current = System.nanoTime();
            }
        }
    }

    public Iterator<T> consume() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // Read terminal state before the queue, including a concurrently published final batch.
                if (isServerFinished() && this.queue.isEmpty()) {
                    return null;
                }
                var iterator = this.queue.poll(200, TimeUnit.MILLISECONDS);
                if (iterator != null) {
                    sendRequest();
                    return iterator;
                }

                // Read terminal state before the queue: a final batch is published with that state.
                if (isServerFinished() && this.queue.isEmpty()) {
                    return null;
                }
                if ((System.nanoTime() - this.current) / 1000_000 > this.timeout) {
                    throw new HgStoreClientException("iterator timeout");
                }

            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return null;
    }

    /**
     * Stop feedback and invoke the request stream's completion or cancellation callback.
     */
    public void clear() {
        boolean finished;
        synchronized (this.stateLock) {
            if (this.closed) {
                return;
            }
            finished = this.terminal == ResultState.FINISHED && this.parsing == 0;
            this.closed = true;
            this.queue.clear();
        }
        if (finished) {
            synchronized (this.requestLock) {
                this.transferComplete.accept(true);
            }
        } else {
            // The transport cancellation hook is thread-safe even during a blocked send.
            this.transferComplete.accept(false);
        }
    }

    @Override
    public void onError(Throwable t) {
        synchronized (this.stateLock) {
            if (this.closed || this.terminal != null) {
                return;
            }
            this.queue.offer(new ErrorMessageIterator<>(t.getMessage()));
            this.terminal = ResultState.ERROR;
            this.current = System.nanoTime();
        }
        log.error("StreamObserver got error:", t);
    }

    @Override
    public void onCompleted() {
        synchronized (this.stateLock) {
            if (!this.closed && this.terminal == null) {
                this.terminal = ResultState.FINISHED;
                this.current = System.nanoTime();
            }
        }
    }

    public void setWatcherQueryId(String queryId) {
        this.queryId = queryId;
    }
}
