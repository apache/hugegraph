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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphKey;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphManagerGraphKeyTest {

    private GraphManager manager;
    private Map<GraphKey, Graph> graphs;

    @Before
    public void setup() {
        this.manager = new GraphManager(new HugeConfig(new PropertiesConfiguration()),
                                        Mockito.mock(EventHub.class));
        this.graphs = Whitebox.getInternalState(this.manager, "graphs");
    }

    @After
    public void teardown() {
        if (this.manager != null) {
            this.manager.close();
        }
    }

    @Test
    public void testSameGraphNameInDifferentSpaces() {
        HugeGraph first = Mockito.mock(HugeGraph.class);
        HugeGraph second = Mockito.mock(HugeGraph.class);
        this.graphs.put(new GraphKey("team", "movies"), first);
        this.graphs.put(new GraphKey("team_other", "movies"), second);

        Assert.assertSame(first, this.manager.graph("team", "movies"));
        Assert.assertSame(first, this.manager.graph("team-movies"));
        Assert.assertSame(second, this.manager.graph("team_other", "movies"));
        Assert.assertEquals(Set.of("movies"), this.manager.graphs("team"));
        Assert.assertEquals(Set.of("movies"), this.manager.graphs("team_other"));
        Assert.assertNull(this.manager.graph("movies"));
        Assert.assertNull(this.manager.graph("unknown-movies"));
    }

    @Test
    public void testGraphNamesViewRemainsLiveAndUnmodifiable() {
        Set<String> names = this.manager.graphs();
        Assert.assertTrue(names.isEmpty());
        this.graphs.put(new GraphKey("DEFAULT", "movies"), Mockito.mock(HugeGraph.class));

        Assert.assertEquals(Set.of("DEFAULT-movies"), names);
        Assert.assertTrue(names.contains("DEFAULT-movies"));
        Assert.assertFalse(names.contains("movies"));
        Assert.assertThrows(UnsupportedOperationException.class, () -> names.add("DEFAULT-other"));
        Assert.assertThrows(UnsupportedOperationException.class, () -> names.remove("DEFAULT-movies"));
        Assert.assertThrows(UnsupportedOperationException.class, names::clear);
        Iterator<String> iterator = names.iterator();
        iterator.next();
        Assert.assertThrows(UnsupportedOperationException.class, iterator::remove);

        this.graphs.remove(new GraphKey("DEFAULT", "movies"));
        Assert.assertTrue(names.isEmpty());
        Assert.assertFalse(names.contains("DEFAULT-movies"));
    }

    @Test
    public void testCombinedNamesPreserveGraphSpaceSeparatorsAndSystemGraph() {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        HugeGraph system = Mockito.mock(HugeGraph.class);
        this.graphs.put(new GraphKey("team-east", "movies"), graph);
        this.graphs.put(new GraphKey("DEFAULT", "~sys_graph"), system);

        Assert.assertSame(graph, this.manager.graph("team-east", "movies"));
        Assert.assertSame(graph, this.manager.graph("team-east-movies"));
        Assert.assertEquals(Set.of("movies"), this.manager.graphs("team-east"));
        Assert.assertSame(system, this.manager.graph("DEFAULT-~sys_graph"));
        Assert.assertTrue(this.manager.graphs().contains("team-east-movies"));
    }

    @Test
    public void testPdLookupUsesExistingStringMetadataKeys() {
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(this.manager, "metaManager", meta);
        Whitebox.setInternalState(this.manager, "PDExist", true);
        Mockito.when(meta.graphConfigs("team")).thenReturn(Map.of("team-movies", Map.of()));
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        this.manager = Mockito.spy(this.manager);
        Mockito.doAnswer(invocation -> {
            this.graphs.put(new GraphKey("team", "movies"), graph);
            return graph;
        }).when(this.manager).createGraph(Mockito.eq("team"), Mockito.eq("movies"),
                                         Mockito.anyString(), Mockito.anyMap(), Mockito.eq(false));

        Assert.assertSame(graph, this.manager.graph("team", "movies"));
        Assert.assertSame(graph, this.manager.graph("team-movies"));
        Mockito.verify(meta).graphConfigs("team");
    }
}
