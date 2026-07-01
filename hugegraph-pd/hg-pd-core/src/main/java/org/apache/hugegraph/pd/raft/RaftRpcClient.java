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

package org.apache.hugegraph.pd.raft;

import java.util.concurrent.CompletableFuture;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftRpcClient {

    protected volatile RpcClient rpcClient;
    private RpcOptions rpcOptions;

    public synchronized boolean init(final RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
        final RaftRpcFactory factory = RpcFactoryHelper.rpcFactory();
        this.rpcClient =
                factory.createRpcClient(factory.defaultJRaftClientConfigHelper(this.rpcOptions));
        return this.rpcClient.init(null);
    }

    /**
     * Request a snapshot
     */
    public CompletableFuture<RaftRpcProcessor.GetMemberResponse>
    getGrpcAddress(final String address) {
        RaftRpcProcessor.GetMemberRequest request = new RaftRpcProcessor.GetMemberRequest();
        FutureClosureAdapter<RaftRpcProcessor.GetMemberResponse> response =
                new FutureClosureAdapter<>();
        internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, response);
        return response.future;
    }

    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint,
                                              final RaftRpcProcessor.BaseRequest request,
                                              final FutureClosureAdapter<V> closure) {
        final InvokeContext invokeCtx = new InvokeContext();
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final RaftRpcProcessor.BaseResponse response =
                            (RaftRpcProcessor.BaseResponse) result;
                    closure.setResponse((V) response);
                } else {
                    closure.failure(err);
                    closure.run(new Status(-1, err.getMessage()));
                }
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback,
                                       this.rpcOptions.getRpcDefaultTimeout());
        } catch (final Throwable t) {
            log.error("failed to call rpc to {}. {}", endpoint, t.getMessage());
            closure.failure(t);
            closure.run(new Status(-1, t.getMessage()));
        }
    }
}
