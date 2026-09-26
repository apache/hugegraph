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

import java.util.HashSet;
import java.util.Set;

import org.lognet.springboot.grpc.GRpcGlobalInterceptor;
import org.springframework.stereotype.Component;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;

/** Waits for terminal application callbacks, not just transport termination. */
@Slf4j
@Component
@GRpcGlobalInterceptor
public class GrpcShutdownBarrier implements ServerInterceptor {

    private final Set<Object> calls = new HashSet<>();
    private boolean closing;

    public synchronized void stopAcceptingCalls() {
        this.closing = true;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        Object token = new Object();
        synchronized (this) {
            if (this.closing) {
                call.close(Status.UNAVAILABLE.withDescription("Store is stopping"), new Metadata());
                return new ServerCall.Listener<ReqT>() { };
            }
            this.calls.add(token);
        }
        try {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                    next.startCall(call, headers)) {
                @Override
                public void onCancel() {
                    try {
                        super.onCancel();
                    } finally {
                        finished(token);
                    }
                }

                @Override
                public void onComplete() {
                    try {
                        super.onComplete();
                    } finally {
                        finished(token);
                    }
                }
            };
        } catch (RuntimeException | Error e) {
            finished(token);
            throw e;
        }
    }

    private synchronized void finished(Object token) {
        this.calls.remove(token);
        this.notifyAll();
    }

    public synchronized void awaitCallbacks() {
        boolean interrupted = false;
        try {
            while (!this.calls.isEmpty()) {
                try {
                    this.wait(5000);
                    if (!this.calls.isEmpty()) {
                        log.warn("Waiting for {} RPC callbacks before closing databases", this.calls.size());
                    }
                } catch (InterruptedException e) {
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
