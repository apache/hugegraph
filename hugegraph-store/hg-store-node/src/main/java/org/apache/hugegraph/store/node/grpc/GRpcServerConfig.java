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

import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.util.HgExecutorUtil;
import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.grpc.ServerBuilder;

/**
 * 2022/3/4
 */
@Component
public class GRpcServerConfig extends GRpcServerBuilderConfigurer {

    public final static String EXECUTOR_NAME = "hg-grpc";
    @Autowired
    private AppConfig appConfig;

    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        AppConfig.ThreadPoolGrpc grpc = appConfig.getThreadPoolGrpc();
        // Terminal callbacks must still be queued when all dispatch threads are busy.
        serverBuilder.executor(
                HgExecutorUtil.createExecutor(EXECUTOR_NAME, grpc.getCore(), grpc.getMax(),
                                              Integer.MAX_VALUE)
        );
    }

}
