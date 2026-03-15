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

package org.apache.hugegraph.pd.util.grpc;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.util.HgExecutorUtil;
import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.grpc.ServerBuilder;

@Component
public class GRpcServerConfig extends GRpcServerBuilderConfigurer {

    public static final String EXECUTOR_NAME = "hg-grpc";
    public static final int MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    @Autowired
    private PDConfig pdConfig;

    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        PDConfig.ThreadPoolGrpc poolGrpc = pdConfig.getThreadPoolGrpc();
        serverBuilder.executor(
                HgExecutorUtil.createExecutor(EXECUTOR_NAME, poolGrpc.getCore(), poolGrpc.getMax(),
                                              poolGrpc.getQueue()));
        serverBuilder.maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE);
        // TODO: GrpcAuthentication is instantiated as a Spring bean but never registered
        // here — add serverBuilder.intercept(grpcAuthentication) once auth is refactored.
    }

}
