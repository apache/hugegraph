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
import org.apache.hugegraph.auth.ContextGremlinServer;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;

import java.util.Map;

import static org.apache.hugegraph.core.GraphManager.DELIMITER;
import static org.apache.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_SERVICE_NAME;

public class HugeGremlinServer {

    private static final Logger LOG = Log.logger(HugeGremlinServer.class);

    /**
     * Construct a ContextGremlinServer and register its EventHub listeners
     * WITHOUT starting it yet. This allows the GRAPH_CREATE listener to be
     * in place before RestServer loads graphs from PD/meta, so that every
     * graph gets injected into Gremlin's global bindings automatically.
     *
     * Call {@link #startPrepared(GremlinServer)} afterwards to actually start.
     */
    public static GremlinServer prepare(String conf, String graphsDir,
                                        EventHub hub) throws Exception {
        LOG.info(GremlinServer.getHeader());
        final Settings settings;
        try {
            settings = Settings.read(conf);
        } catch (Exception e) {
            LOG.error("Can't found the configuration file at '{}' or " +
                      "being parsed properly. [{}]", conf, e.getMessage());
            throw e;
        }
        E.checkState(settings.graphs != null,
                     "The GremlinServer's settings.graphs is null");
        if (graphsDir != null) {
            Map<String, String> configs = ConfigUtil.scanGraphsDir(graphsDir);
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                String key = String.join(DELIMITER, DEFAULT_GRAPH_SPACE_SERVICE_NAME,
                        entry.getKey());
                settings.graphs.put(key, entry.getValue());
            }
        }

        LOG.info("Configuring Gremlin Server from {}", conf);
        // Constructing ContextGremlinServer registers GRAPH_CREATE/GRAPH_DROP
        // listeners on the hub immediately, before any graphs are loaded.
        ContextGremlinServer server = new ContextGremlinServer(settings, hub);

        // Inject traversal sources for graphs in static config files
        server.injectTraversalSource();

        return server;
    }

    /**
     * Start a ContextGremlinServer that was previously prepared via
     * {@link #prepare(String, String, EventHub)}.
     */
    public static GremlinServer startPrepared(GremlinServer server)
            throws Exception {
        server.start().exceptionally(t -> {
            LOG.error("Gremlin Server was unable to start and will " +
                      "shutdown now: {}", t.getMessage());
            server.stop().join();
            throw new HugeException("Failed to start Gremlin Server");
        }).join();
        return server;
    }

    /**
     * Convenience method: prepare and start in one call (original behavior,
     * kept for backward compatibility with any other callers).
     */
    public static GremlinServer start(String conf, String graphsDir,
                                      EventHub hub) throws Exception {
        return startPrepared(prepare(conf, graphsDir, hub));
    }
}
