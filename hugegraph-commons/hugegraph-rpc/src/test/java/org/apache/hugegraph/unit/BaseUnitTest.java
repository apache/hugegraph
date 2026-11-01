/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit;

import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;

import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.server.Server;
import io.netty.channel.ChannelFuture;

public class BaseUnitTest {

    @BeforeClass
    public static void initEnv() {
        OptionSpace.register("rpc", "org.apache.hugegraph.config.RpcOptions");
    }

    protected static HugeConfig config(boolean server) {
        return config(server ? "server" : "client");
    }

    protected static HugeConfig config(String type) {
        String name = String.format("rpc-%s.properties", type);
        URL conf = BaseUnitTest.class.getClassLoader().getResource(name);
        return new HugeConfig(Paths.get(URI.create(conf.toExternalForm())).toString());
    }

    protected static void startServer(RpcServer rpcServer) {
        rpcServer.config().configs().values().forEach(c -> {
            c.setRepeatedExportLimit(100);
        });

        rpcServer.exportAll();
    }

    protected static void stopServer(RpcServer rpcServer) {
        closeServer(rpcServer, rpcServer::destroy);
    }

    protected static void unexportServer(RpcServer rpcServer) {
        closeServer(rpcServer, rpcServer::unexportAll);
    }

    protected static ChannelFuture listener(RpcServer rpcServer) {
        if (!rpcServer.enabled()) {
            return null;
        }
        ServerConfig config = Whitebox.getInternalState(rpcServer, "serverConfig");
        Server server = config.getServer();
        if (server == null || !server.isStarted()) {
            return null;
        }
        Object remoting = Whitebox.getInternalState(server, "remotingServer");
        return Whitebox.getInternalState(remoting, "channelFuture");
    }

    private static void closeServer(RpcServer rpcServer, Runnable close) {
        ChannelFuture binding = listener(rpcServer);
        close.run();
        if (binding != null) {
            // Bolt stops asynchronously; the next fixture may reuse this port.
            ChannelFuture closed = binding.channel().closeFuture();
            Assert.assertTrue(closed.awaitUninterruptibly(5, TimeUnit.SECONDS));
            Assert.assertTrue(closed.isSuccess());
        }
    }
}
