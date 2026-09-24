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

package org.apache.hugegraph.store.node.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.CheckForNull;

import lombok.extern.slf4j.Slf4j;

/**
 * Golang style channel without buffering
 * 2022/2/28
 *
 * @version 1.1 on 2022/04/02
 */
@Slf4j
public final class HgChannel<T> {

    private final BlockingQueue<Supplier<T>> queue;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long timeoutSeconds;

    private HgChannel(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        this.queue = new SynchronousQueue();
    }

    public static HgChannel of() {
        return new HgChannel(Long.MAX_VALUE);
    }

    public static HgChannel of(long timeoutSeconds) {
        return new HgChannel(timeoutSeconds);
    }

    /**
     * @param t
     * @return true if send successfully, false when it is timeout.
     * @throws IllegalArgumentException when the argument t is null.
     * @throws IllegalStateException    when the chan has been closed.
     * @throws RuntimeException         when InterruptedException happen
     */
    public boolean send(T t) {
        if (t == null) {
            throw new IllegalArgumentException("the argument t is null");
        }
        synchronized (this.queue) {
            if (this.closed.get()) {
                return false;
            }
            boolean flag;
            try {
                flag = this.queue.offer(() -> t, timeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("failed to send a item to chan. cause by: ", t);
                throw new RuntimeException(e);
            }
            return flag;
        }
    }

    /**
     * return an item from the chan
     *
     * @throws RuntimeException when fail to receive an item.
     */
    @CheckForNull
    public T receive() {
        return receive((timeout) -> {
            throw new RuntimeException("Timeout, max time: " + timeout + " seconds;");
        });
    }

    /**
     * return an item from the chan
     *
     * @return null when the chan has been closed
     * @throws RuntimeException
     */
    @CheckForNull
    public T receive(Consumer<Long> timeoutCallBack) {
        Supplier<T> s;
        synchronized (this.closed) {
            if (this.closed.get()) {
                s = this.queue.poll();
            } else {
                try {
                    s = this.queue.poll(timeoutSeconds, TimeUnit.SECONDS);
                } catch (Throwable t) {
                    log.error("Failed to receive a item from chan. cause by: ", t);
                    throw new RuntimeException(t);
                }
                if (s == null) {
                    if (timeoutCallBack == null) {
                        throw new RuntimeException(
                                "Timeout, max time: " + timeoutSeconds + " seconds;");
                    } else {
                        timeoutCallBack.accept(timeoutSeconds);
                    }
                }
            }
        }
        if (s == null) {
            return null;
        } else {
            return s.get();
        }
    }

    public boolean isClosed() {
        return this.closed.get();
    }

    /**
     * @throws RuntimeException when fail to close the chan
     */
    public void close() {
        if (this.closed.get()) {
            return;
        }
        this.closed.set(true);
        this.queue.offer(() -> null);
        Thread.yield();
        this.queue.poll();

    }

}
