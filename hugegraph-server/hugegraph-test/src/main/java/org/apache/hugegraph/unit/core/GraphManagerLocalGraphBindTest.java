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

package org.apache.hugegraph.unit.core;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.ContextGremlinServer;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Events;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Gremlin Server opens conf/graphs on its own, once, and drops a graph whose
 * open fails there. The REST load of the same graph has to offer it to
 * Gremlin again, or that Server answers REST while every Gremlin call on the
 * graph fails until it restarts (#3228).
 */
public class GraphManagerLocalGraphBindTest {

    private File graphsDir;
    private EventHub hub;
    private GraphManager manager;
    private ContextGremlinServer gremlin;
    private List<String> created;
    private EventListener listener;

    @Before
    public void setup() throws IOException {
        this.graphsDir = Files.createTempDirectory("local-graph-bind").toFile();
        this.hub = new EventHub("local-graph-bind-test");
        this.manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()), this.hub);
        this.created = new CopyOnWriteArrayList<>();
        this.listener = event -> {
            event.checkArgs(HugeGraph.class);
            this.created.add(((HugeGraph) event.args()[0]).spaceGraphName());
            return null;
        };
        this.hub.listen(Events.GRAPH_CREATE, this.listener);
    }

    @After
    public void teardown() throws Exception {
        List<HugeGraph> graphs = new ArrayList<>();
        for (String name : this.manager.graphs()) {
            graphs.add(this.manager.graph(name));
        }
        try {
            if (this.gremlin != null) {
                this.gremlin.stop().get(30, TimeUnit.SECONDS);
            }
            this.hub.unlisten(Events.GRAPH_CREATE, this.listener);
            this.manager.close();
        } finally {
            graphs.forEach(HugeFactory::remove);
            FileUtils.deleteQuietly(this.graphsDir);
        }
    }

    @Test
    public void testLocalLoadOffersGraphToGremlin() throws IOException {
        String path = this.writeGraphConf("bind_local", "bind_local");

        this.manager.loadGraphsFromLocal(ImmutableMap.of("bind_local", path));

        Assert.assertEquals(1, this.created.size());
        Assert.assertEquals("DEFAULT-bind_local", this.created.get(0));
        Assert.assertNotNull(this.manager.graph("DEFAULT", "bind_local"));
    }

    @Test
    public void testLoadingTwiceRebindsTheSameGraph() throws IOException {
        String path = this.writeGraphConf("bind_twice", "bind_twice");
        Map<String, String> confs = ImmutableMap.of("bind_twice", path);

        this.manager.loadGraphsFromLocal(confs);
        HugeGraph first = this.manager.graph("DEFAULT", "bind_twice");
        this.manager.loadGraphsFromLocal(confs);

        Assert.assertEquals(2, this.created.size());
        Assert.assertSame(first, this.manager.graph("DEFAULT", "bind_twice"));
    }

    @Test
    public void testGraphNamedDifferentlyByConfigIsNotRebound() throws IOException {
        String path = this.writeGraphConf("bind_file", "bind_store");

        this.manager.loadGraphsFromLocal(ImmutableMap.of("bind_file", path));

        Assert.assertTrue(this.created.isEmpty());
        Assert.assertNotNull(this.manager.graph("DEFAULT", "bind_file"));
    }

    @Test
    public void testFailedBindKeepsGraphLoaded() throws IOException {
        String path = this.writeGraphConf("bind_failed", "bind_failed");
        EventListener failing = event -> {
            throw new IllegalStateException("gremlin binding failed");
        };
        this.hub.listen(Events.GRAPH_CREATE, failing);

        try {
            this.manager.loadGraphsFromLocal(ImmutableMap.of("bind_failed", path));
        } finally {
            this.hub.unlisten(Events.GRAPH_CREATE, failing);
        }

        Assert.assertEquals(1, this.created.size());
        Assert.assertNotNull(this.manager.graph("DEFAULT", "bind_failed"));
    }

    @Test
    public void testGraphMissingFromGremlinGetsBound() throws IOException {
        String path = this.writeGraphConf("bind_missing", "bind_missing");
        // No settings.graphs entry stands in for a failed static open
        this.gremlin = new ContextGremlinServer(newSettings(), this.hub);
        org.apache.tinkerpop.gremlin.server.GraphManager bindings =
                this.gremlin.getServerGremlinExecutor().getGraphManager();
        Assert.assertNull(bindings.getGraph("DEFAULT-bind_missing"));

        this.manager.loadGraphsFromLocal(ImmutableMap.of("bind_missing", path));

        Assert.assertSame(this.manager.graph("DEFAULT", "bind_missing"),
                          bindings.getGraph("DEFAULT-bind_missing"));
        Assert.assertNotNull(bindings.getTraversalSource("__g_DEFAULT-bind_missing"));
    }

    @Test
    public void testGraphOpenedByGremlinKeepsItsInstance() throws IOException {
        String path = this.writeGraphConf("bind_static", "bind_static");
        Settings settings = newSettings();
        settings.graphs.put("DEFAULT-bind_static", path);
        this.gremlin = new ContextGremlinServer(settings, this.hub);
        org.apache.tinkerpop.gremlin.server.GraphManager bindings =
                this.gremlin.getServerGremlinExecutor().getGraphManager();
        Graph opened = bindings.getGraph("DEFAULT-bind_static");
        Assert.assertNotNull(opened);

        this.manager.loadGraphsFromLocal(ImmutableMap.of("bind_static", path));

        // The rebind ran and handed Gremlin the instance it already held
        Assert.assertEquals(ImmutableList.of("DEFAULT-bind_static"), this.created);
        Assert.assertSame(opened, bindings.getGraph("DEFAULT-bind_static"));
        Assert.assertSame(opened, this.manager.graph("DEFAULT", "bind_static"));
        Assert.assertSame(opened, bindings.getTraversalSource("__g_DEFAULT-bind_static")
                                          .getGraph());
    }

    private String writeGraphConf(String fileName, String store)
                                  throws IOException {
        File conf = new File(this.graphsDir, fileName + ".properties");
        String content = "gremlin.graph=org.apache.hugegraph.HugeFactory\n" +
                         "backend=memory\n" +
                         "serializer=text\n" +
                         "store=" + store + "\n";
        FileUtils.writeStringToFile(conf, content, StandardCharsets.UTF_8);
        return conf.getPath();
    }

    private static Settings newSettings() {
        Settings settings = new Settings();
        settings.host = "127.0.0.1";
        settings.port = 0;
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        return settings;
    }
}
