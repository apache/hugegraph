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

import org.junit.BeforeClass;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rpc.RpcServer;

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
        rpcServer.destroy();
    }
}
