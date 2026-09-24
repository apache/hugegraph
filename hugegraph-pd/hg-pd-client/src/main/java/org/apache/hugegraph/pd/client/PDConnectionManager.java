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

package org.apache.hugegraph.pd.client;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDRuntimeException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractStub;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class PDConnectionManager {

    private final static long WAITING_SECONDS = 3;
    private final static ExecutorService reconnectExecutor =
            newFixedThreadPool(1, "pdcm-reconnect-%d");
    private final static ExecutorService taskExecutor = newFixedThreadPool(1, "pdcm-task-%d");
    private final PDConfig config;
    private final Supplier<String> leaderSupplier;
    private final List<Runnable> reconnectionTasks = new CopyOnWriteArrayList<>();

    private static ExecutorService newFixedThreadPool(int nThreads, String name) {
        return Executors.newFixedThreadPool(nThreads,
                                            new ThreadFactoryBuilder().setDaemon(true)
                                                                      .setNameFormat(name).build());
    }

    PDConnectionManager(PDConfig config, Supplier<String> leaderSupplier) {
        this.config = config;
        this.leaderSupplier = leaderSupplier;
    }

    public void addReconnectionTask(Runnable task) {
        this.reconnectionTasks.add(task);
    }

    public void forceReconnect() {
        tryTask(reconnectExecutor, this::doReconnect, "Force Reconnection");
    }

    private void doReconnect() {
        log.info("[PDCM] Trying to force reconnect...");
        this.reconnectionTasks.stream().forEach(
                (e) -> {
                    try {
                        log.info("[PDCM] Force reconnection task...");
                        e.run();
                    } catch (Exception ex) {
                        log.error("[PDCM] Failed to run the reconnection task, caused by:", ex);
                    }
                });
    }

    /**
     * Create a new stub with the leader channel and the async params
     */
    public <T extends AbstractStub<T>> T newStub(Function<ManagedChannel, T> stubCreator) {
        HgAssert.isArgumentNotNull(stubCreator, "The stub creator can't be null");
        return newStub(stubCreator, getChannel());
    }

    private <T extends AbstractStub<T>> T newStub(Function<ManagedChannel, T> creator,
                                                  ManagedChannel channel) {
        return AbstractClient.setAsyncParams(creator.apply(channel), this.config);
    }

    ManagedChannel getChannel() {
        ManagedChannel channel = null;
        try {
            channel = Channels.getChannel(tryGetLeader());
        } catch (Exception e) {
            log.error("[PDCM] Failed to get the leader channel, caused by:", e);
            throw new PDRuntimeException(-1, "[PDCM] Failed to get the channel, caused by:", e);
        }

        return channel;
    }

    String tryGetLeader() {
        log.info("[PDCM] Trying to get the PD leader...");
        String leader =
                tryTask(taskExecutor, () -> this.leaderSupplier.get(), "Getting PD Leader IP");
        if (leader == null) {
            throw new PDRuntimeException(-1, "[PDCM] Failed to get the PD leader.");
        }
        log.info("[PDCM] Get the PD leader: [ {} ]", leader);
        return leader;
    }

    static void tryTask(ExecutorService executor, Runnable task, String taskName) {
        tryTask(executor, () -> {
            task.run();
            return true;
        }, taskName);
    }

    static <T> T tryTask(ExecutorService executor, Callable<T> task, String taskName) {
        Future<T> future = executor.submit(task);
        T result = null;

        try {
            result = future.get(WAITING_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("[PDCM] Task [ {} ] interrupted. error:", taskName, e);
        } catch (ExecutionException e) {
            log.error("[PDCM] Task [ {} ] execution failed.", taskName, e);
        } catch (TimeoutException e) {
            log.error("[PDCM] Task [ {} ] did not complete within the specified timeout: [ {} ]",
                      taskName, WAITING_SECONDS);
            future.cancel(true);
        }

        return result;
    }
}
