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

package org.apache.hugegraph;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.type.define.SerialEnum;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;

public class HugeFactory {

    public static final String SYS_GRAPH = Graph.Hidden.hide("sys_graph");
    private static final Logger LOG = Log.logger(HugeFactory.class);
    private static final String NAME_REGEX = "^[A-Za-z][A-Za-z0-9_]{0,47}$";
    private static final Map<String, HugeGraph> GRAPHS = new HashMap<>();
    private static final AtomicBoolean SHUT_DOWN = new AtomicBoolean(false);
    private static final Thread SHUT_DOWN_HOOK = new Thread(() -> {
        LOG.info("HugeGraph is shutting down");
        HugeFactory.shutdown(30L, true);
    }, "hugegraph-shutdown");

    static {
        SerialEnum.registerInternalEnums();
        HugeGraph.registerTraversalStrategies(StandardHugeGraph.class);

        Runtime.getRuntime().addShutdownHook(SHUT_DOWN_HOOK);
    }

    public static synchronized HugeGraph open(Configuration config) {
        HugeConfig conf = config instanceof HugeConfig ?
                          (HugeConfig) config : new HugeConfig(config);
        return open(conf);
    }

    public static synchronized HugeGraph open(HugeConfig config) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // Not allowed to read file via Gremlin when SecurityManager enabled
            String configFileName;
            File configFile = config.file();
            if (configFile == null) {
                configFileName = config.toString();
            } else {
                configFileName = configFile.getName();
            }
            sm.checkRead(configFileName);
        }

        String name = config.get(CoreOptions.STORE);
        checkGraphName(name, "graph config(like hugegraph.properties)");
        String graphSpace = config.get(CoreOptions.GRAPH_SPACE);
        name = name.toLowerCase();
        String spaceGraphName = graphSpace + "-" + name;
        HugeGraph graph = GRAPHS.get(spaceGraphName);
        if (graph == null || graph.closed()) {
            graph = new StandardHugeGraph(config);
            GRAPHS.put(spaceGraphName, graph);
        } else {
            String backend = config.get(CoreOptions.BACKEND);
            E.checkState(backend.equalsIgnoreCase(graph.backend()),
                         "Graph name '%s' has been used by backend '%s'",
                         name, graph.backend());
        }
        return graph;
    }

    public static HugeGraph open(String path) {
        return open(getLocalConfig(path));
    }

    public static HugeGraph open(URL url) {
        return open(getRemoteConfig(url));
    }

    public static void remove(HugeGraph graph) {
        String spaceGraphName = graph.spaceGraphName();
        GRAPHS.remove(spaceGraphName);
    }

    public static void checkGraphName(String name, String configFile) {
        E.checkArgument(SYS_GRAPH.equals(name) || name.matches(NAME_REGEX),
                        "Invalid graph name '%s' in %s, " +
                        "valid graph name is up to 48 alpha-numeric " +
                        "characters and underscores and only letters are " +
                        "supported as first letter. " +
                        "Note: letter is case insensitive", name, configFile);
    }

    public static PropertiesConfiguration getLocalConfig(String path) {
        File file = new File(path);
        E.checkArgument(file.exists() && file.isFile() && file.canRead(),
                        "Please specify a proper config file rather than: %s",
                        file.toString());
        try {
            return new Configurations().properties(file);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load config file: %s", e, path);
        }
    }

    public static PropertiesConfiguration getRemoteConfig(URL url) {
        try {
            return new Configurations().properties(url);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load remote config file: %s",
                                    e, url);
        }
    }

    public static void shutdown(long timeout) {
        shutdown(timeout, false);
    }

    /**
     * Stop all the daemon threads
     *
     * @param timeout         wait in seconds
     * @param ignoreException don't throw exception if true
     */
    public static void shutdown(long timeout, boolean ignoreException) {
        if (!SHUT_DOWN.compareAndSet(false, true)) {
            return;
        }
        try {

            if (!EventHub.destroy(timeout)) {
                throw new TimeoutException(timeout + "s");
            }
            TaskManager.instance().shutdown(timeout);
            OltpTraverser.destroy();
        } catch (Throwable e) {
            LOG.error("Error while shutdown", e);
            SHUT_DOWN.compareAndSet(true, false);
            if (ignoreException) {
                return;
            } else {
                throw new HugeException("Failed to shutdown", e);
            }
        }

        LOG.info("HugeFactory shutdown");
    }

    public static void removeShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(SHUT_DOWN_HOOK);
    }
}
