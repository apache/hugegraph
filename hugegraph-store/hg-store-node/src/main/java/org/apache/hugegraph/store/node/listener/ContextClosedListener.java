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

package org.apache.hugegraph.store.node.listener;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.node.grpc.GrpcShutdownBarrier;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.lognet.springboot.grpc.context.GRpcServerInitializedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import io.grpc.Server;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ContextClosedListener implements ApplicationListener<ContextClosedEvent> {

    private final List<Server> grpcServers = new CopyOnWriteArrayList<>();

    @Autowired
    HgStoreStreamImpl storeStream;
    @Autowired
    TTLCleaner cleaner;
    @Autowired
    GrpcShutdownBarrier grpcBarrier;

    @EventListener
    public void onServerInitialized(GRpcServerInitializedEvent event) {
        this.grpcServers.add(event.getServer());
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        // Spring invokes HgStoreNodeService.destroy() after this event. Raft and
        // its databases must remain available until local workers have stopped.
        this.grpcBarrier.stopAcceptingCalls();
        if (storeStream != null) {
            storeStream.stopAcceptingScans();
        }
        this.grpcServers.forEach(Server::shutdownNow);
        if (cleaner != null) {
            // The scheduler can create the worker pool while a job is starting.
            stopAndWait(cleaner.getScheduler(), "TTL scheduler");
            stopAndWait(cleaner.getExecutor(), "TTL workers");
        }
        if (storeStream != null) {
            // Cancelled queued scans must run their finally blocks to release iterators.
            storeStream.shutdownScans();
            awaitWorkers(storeStream.getRealExecutor(), "scan workers");
        }
        boolean interrupted = false;
        try {
            for (Server server : this.grpcServers) {
                while (!server.isTerminated()) {
                    try {
                        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                            log.warn("Still waiting for gRPC callbacks before closing databases");
                        }
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        this.grpcBarrier.awaitCallbacks();
        log.info("closed gRPC callbacks, scan and TTL workers");
    }

    private static void stopAndWait(ExecutorService executor, String name) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        awaitWorkers(executor, name);
    }

    private static void awaitWorkers(ExecutorService executor, String name) {
        if (executor == null) {
            return;
        }
        boolean interrupted = false;
        try {
            while (!executor.isTerminated()) {
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Still waiting for {} to stop before closing databases", name);
                    }
                } catch (InterruptedException e) {
                    // An interrupted shutdown thread must not close databases underneath
                    // a worker that still owns a native iterator.
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
