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

import org.lognet.springboot.grpc.context.GRpcServerInitializedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import io.grpc.Server;

/**
 * Reports process startup only. This is not a shard or data readiness check.
 */
@Component
public class StoreProcessLifecycle {

    private final ApplicationContext context;
    private boolean applicationReady;
    private boolean stopping;
    private Server grpcServer;

    public StoreProcessLifecycle(ApplicationContext context) {
        this.context = context;
    }

    @EventListener
    public synchronized void onGrpcStarted(GRpcServerInitializedEvent event) {
        if (event.getApplicationContext() == this.context && !this.stopping) {
            this.grpcServer = event.getServer();
        }
    }

    @EventListener
    public synchronized void onApplicationReady(ApplicationReadyEvent event) {
        if (event.getApplicationContext() == this.context && !this.stopping) {
            this.applicationReady = true;
        }
    }

    @EventListener
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public synchronized void onContextClosed(ContextClosedEvent event) {
        if (event.getApplicationContext() == this.context) {
            this.stopping = true;
        }
    }

    public synchronized String getState() {
        if (this.stopping) {
            return "STOPPING";
        }
        if (this.grpcServer != null &&
            (this.grpcServer.isShutdown() || this.grpcServer.isTerminated())) {
            return "UNAVAILABLE";
        }
        return this.applicationReady && this.grpcServer != null ? "STARTED" : "STARTING";
    }
}
