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

package org.apache.hugegraph.dist;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.constant.ServiceConstant;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.LogManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public class HugeGraphServer {

    private static final Logger LOG = Log.logger(HugeGraphServer.class);

    private final RestServer restServer;
    private final GremlinServer gremlinServer;
    private final MemoryMonitor memoryMonitor;
    private final MetaManager metaManager = MetaManager.instance();

    public static void register() {
        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();
    }

    public HugeGraphServer(String gremlinServerConf, String restServerConf)
            throws Exception {
        // Only switch on security manager after HugeGremlinServer started
        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(null);
        RestServer restServer = null;
        GremlinServer preparedGremlinServer = null;
        GremlinServer gremlinServer = null;
        MemoryMonitor memoryMonitor = null;
        try {
            ConfigUtil.checkGremlinConfig(gremlinServerConf);
            HugeConfig restServerConfig = new HugeConfig(restServerConf);
            String graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
            EventHub hub = new EventHub("gremlin=>hub<=rest");

            PdMetaDriver.PDAuthConfig.setAuthority(
                    ServiceConstant.SERVICE_NAME,
                    ServiceConstant.AUTHORITY);

            // Prepare GremlinServer (registers GRAPH_CREATE listener) BEFORE
            // RestServer starts loading graphs from PD/meta. This ensures that
            // graphs loaded during RestServer initialization are captured by
            // ContextGremlinServer's listener and injected into Gremlin's bindings.
            try {
                preparedGremlinServer = HugeGremlinServer.prepare(
                        gremlinServerConf, graphsDir, hub);
            } catch (Throwable e) {
                LOG.error("HugeGremlinServer prepare error: ", e);
                throw e;
            }

            try {
                // Start HugeRestServer (loads graphs from PD; events go to
                // ContextGremlinServer which is already listening)
                restServer = HugeRestServer.start(restServerConf, hub);
            } catch (Throwable e) {
                LOG.error("HugeRestServer start error: ", e);
                stopPreparedGremlinServer(preparedGremlinServer);
                throw e;
            }

            try {
                // Now start the pre-prepared GremlinServer
                gremlinServer = HugeGremlinServer.startPrepared(
                        preparedGremlinServer);
            } catch (Throwable e) {
                LOG.error("HugeGremlinServer start error: ", e);
                try {
                    restServer.shutdown().get();
                } catch (Throwable t) {
                    LOG.error("HugeRestServer stop error: ", t);
                }
                throw e;
            }

            try {
                // Start (In-Heap) Memory Monitor
                memoryMonitor = new MemoryMonitor(restServerConf);
                memoryMonitor.start();
            } catch (Throwable e) {
                LOG.error("MemoryMonitor start error: ", e);
                rollbackStartup(memoryMonitor, gremlinServer, restServer);
                throw e;
            }
        } finally {
            System.setSecurityManager(securityManager);
        }
        this.restServer = restServer;
        this.gremlinServer = gremlinServer;
        this.memoryMonitor = memoryMonitor;
    }

    private static void stopPreparedGremlinServer(GremlinServer server) {
        if (server == null) {
            return;
        }
        try {
            server.stop().get();
        } catch (Throwable t) {
            LOG.error("HugeGremlinServer stop error: ", t);
        }
    }

    static void rollbackStartup(MemoryMonitor monitor,
                                GremlinServer gremlinServer,
                                RestServer restServer) {
        if (monitor != null) {
            try {
                monitor.stop();
            } catch (Throwable t) {
                LOG.error("MemoryMonitor stop error: ", t);
            }
        }

        stopPreparedGremlinServer(gremlinServer);

        if (restServer != null) {
            try {
                restServer.shutdown().get();
            } catch (Throwable t) {
                LOG.error("HugeRestServer stop error: ", t);
            }
        }
    }

    public void stop() {
        this.memoryMonitor.stop();

        try {
            this.gremlinServer.stop().get();
            LOG.info("HugeGremlinServer stopped");
        } catch (Throwable e) {
            LOG.error("HugeGremlinServer stop error: ", e);
        }

        try {
            this.restServer.shutdown().get();
            LOG.info("HugeRestServer stopped");
        } catch (Throwable e) {
            LOG.error("HugeRestServer stop error: ", e);
        }

        try {
            HugeFactory.shutdown(30L);
            LOG.info("HugeGraph stopped");
        } catch (Throwable e) {
            LOG.error("Failed to stop HugeGraph: ", e);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            String msg = "Start HugeGraphServer need to pass 2 parameters, " +
                         "they are the config files of GremlinServer and " +
                         "RestServer, for example: conf/gremlin-server.yaml " +
                         "conf/rest-server.properties";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        HugeGraphServer.register();

        HugeGraphServer server;
        try {
            server = new HugeGraphServer(args[0], args[1]);
        } catch (Throwable e) {
            HugeFactory.shutdown(30L, true);
            throw e;
        }

        /*
         * Remove HugeFactory.shutdown and let HugeGraphServer.stop() do it.
         * NOTE: HugeFactory.shutdown hook may be invoked before server stop,
         * causes event-hub can't execute notification events for another
         * shutdown executor such as gremlin-stop-shutdown
         */
        HugeFactory.removeShutdownHook();

        CompletableFuture<?> serverStopped = new CompletableFuture<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("HugeGraphServer stopping");
            server.stop();
            LOG.info("HugeGraphServer stopped");

            LogManager.shutdown();
            serverStopped.complete(null);
        }, "hugegraph-server-shutdown"));
        // Wait for server-shutdown and server-stopped
        serverStopped.get();
    }
}
