/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.GraphStatus;
import org.junit.Test;

public class GraphStatusAggregateTest {

    private static final String STATUS_KEY = "status";
    private static final String READY_COUNT_KEY = "ready_count";
    private static final String TOTAL_COUNT_KEY = "total_count";
    private static final String EXPECTED_COUNT_KEY = "expected_count";
    private static final String SERVERS_KEY = "servers";
    private static final String SERVER_KEY = "server";

    private static final String STATUS_UNKNOWN = "UNKNOWN";

    @Test
    public void testEmptyEntriesRenderUnknownStatus() {
        GraphStatusAggregate aggregate =
                GraphStatusAggregate.of(Collections.emptyList(), 0);

        Assert.assertNull(aggregate.status());
        Assert.assertEquals(0, aggregate.readyCount());
        Assert.assertEquals(0, aggregate.totalCount());
        Assert.assertTrue(aggregate.entries().isEmpty());

        Map<String, Object> map = aggregate.asMap();
        Assert.assertEquals(STATUS_UNKNOWN, map.get(STATUS_KEY));
        Assert.assertEquals(0, map.get(READY_COUNT_KEY));
        Assert.assertEquals(0, map.get(TOTAL_COUNT_KEY));
        Assert.assertTrue(servers(map).isEmpty());
    }

    @Test
    public void testUnknownExpectedIsNotReady() {
        // The number of servers that should report is unknown, so a server
        // that did not report yet can't be told from one that doesn't exist
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", GraphStatus.READY));

        for (int expected : new int[]{-5, 0}) {
            GraphStatusAggregate aggregate =
                    GraphStatusAggregate.of(entries, expected);

            Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
            Assert.assertFalse(aggregate.expectedKnown());
            Assert.assertEquals(2, aggregate.readyCount());
            Assert.assertEquals(2, aggregate.totalCount());

            Map<String, Object> map = aggregate.asMap();
            Assert.assertEquals(GraphStatus.LOADING.name(),
                                map.get(STATUS_KEY));
            // The unknown number is rendered rather than dropped, a client
            // that only reads the status must not have to notice a gap
            Assert.assertTrue(map.containsKey(EXPECTED_COUNT_KEY));
            Assert.assertNull(map.get(EXPECTED_COUNT_KEY));
        }
    }

    @Test
    public void testNullEntriesRenderUnknownStatus() {
        GraphStatusAggregate aggregate = GraphStatusAggregate.of(null, 3);

        Assert.assertNull(aggregate.status());
        Assert.assertEquals(0, aggregate.totalCount());
        Assert.assertEquals(STATUS_UNKNOWN,
                            aggregate.asMap().get(STATUS_KEY));
    }

    @Test
    public void testNullEntryIsIgnored() {
        List<GraphStatusEntry> entries = new ArrayList<>();
        entries.add(entry("server-1", GraphStatus.READY));
        entries.add(null);

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 1);

        Assert.assertEquals(GraphStatus.READY, aggregate.status());
        Assert.assertEquals(1, aggregate.readyCount());
        Assert.assertEquals(1, aggregate.totalCount());
    }

    @Test
    public void testSingleReadyWithUnknownExpectedIsLoading() {
        // A readiness gate has to fail closed: a single server that reported
        // ready is not a ready cluster while the size of the cluster is
        // unknown, and the caller only reads the status
        List<GraphStatusEntry> entries =
                Collections.singletonList(entry("server-1",
                                                GraphStatus.READY));

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 0);

        Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
        Assert.assertEquals(1, aggregate.readyCount());
        Assert.assertEquals(1, aggregate.totalCount());
        Assert.assertEquals(0, aggregate.expected());
        Assert.assertFalse(aggregate.expectedKnown());
        Assert.assertEquals(GraphStatus.LOADING.name(),
                            aggregate.asMap().get(STATUS_KEY));
    }

    @Test
    public void testAllReadyBelowExpectedIsLoading() {
        // The servers that reported are all ready, but one of the three
        // servers of the cluster has not reported yet, so the graph is not
        // ready cluster wide
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", GraphStatus.READY));

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 3);

        Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
        Assert.assertEquals(2, aggregate.readyCount());
        Assert.assertEquals(2, aggregate.totalCount());
        Assert.assertEquals(3, aggregate.expected());

        Map<String, Object> map = aggregate.asMap();
        Assert.assertEquals(GraphStatus.LOADING.name(), map.get(STATUS_KEY));
        Assert.assertEquals(2, map.get(READY_COUNT_KEY));
        Assert.assertEquals(2, map.get(TOTAL_COUNT_KEY));
        Assert.assertEquals(3, map.get(EXPECTED_COUNT_KEY));
    }

    @Test
    public void testReadyCountEqualToExpectedIsReady() {
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", GraphStatus.READY));

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 2);

        Assert.assertEquals(GraphStatus.READY, aggregate.status());
        Assert.assertEquals(2, aggregate.readyCount());
        Assert.assertEquals(2, aggregate.expected());
    }

    @Test
    public void testReadyCountAboveExpectedIsReady() {
        // A server that was scaled down may still be counted as expected
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", GraphStatus.READY),
                              entry("server-3", GraphStatus.READY));

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 2);

        Assert.assertEquals(GraphStatus.READY, aggregate.status());
        Assert.assertEquals(3, aggregate.readyCount());
        Assert.assertEquals(3, aggregate.totalCount());
    }

    @Test
    public void testFailedOverridesLoadingAndReady() {
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", GraphStatus.LOADING),
                              entry("server-3", GraphStatus.FAILED));

        for (int expected : new int[]{-1, 0, 1, 3, 9}) {
            GraphStatusAggregate aggregate =
                    GraphStatusAggregate.of(entries, expected);

            Assert.assertEquals(GraphStatus.FAILED, aggregate.status());
            Assert.assertEquals(1, aggregate.readyCount());
            Assert.assertEquals(3, aggregate.totalCount());
            Assert.assertEquals(GraphStatus.FAILED.name(),
                                aggregate.asMap().get(STATUS_KEY));
        }
    }

    @Test
    public void testLoadingOverridesReady() {
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", GraphStatus.LOADING));

        for (int expected : new int[]{-1, 0, 1, 2}) {
            GraphStatusAggregate aggregate =
                    GraphStatusAggregate.of(entries, expected);

            Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
            Assert.assertEquals(1, aggregate.readyCount());
            Assert.assertEquals(2, aggregate.totalCount());
        }
    }

    @Test
    public void testEntryWithNullStatusIsNotReady() {
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-1", GraphStatus.READY),
                              entry("server-2", null));

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 2);

        Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
        Assert.assertEquals(1, aggregate.readyCount());
        Assert.assertEquals(2, aggregate.totalCount());

        // An unset status stays unset in the rendered per server map
        Assert.assertNull(servers(aggregate.asMap()).get(1).get(STATUS_KEY));
    }

    @Test
    public void testExpectedCountAlwaysRendered() {
        List<GraphStatusEntry> entries =
                Collections.singletonList(entry("server-1",
                                                GraphStatus.READY));

        Map<String, Object> known =
                GraphStatusAggregate.of(entries, 2).asMap();
        Assert.assertTrue(known.containsKey(EXPECTED_COUNT_KEY));
        Assert.assertEquals(2, known.get(EXPECTED_COUNT_KEY));

        // An unknown number is rendered as null rather than left out, so a
        // client can tell it apart from a number it failed to read
        Map<String, Object> zero = GraphStatusAggregate.of(entries, 0).asMap();
        Assert.assertTrue(zero.containsKey(EXPECTED_COUNT_KEY));
        Assert.assertNull(zero.get(EXPECTED_COUNT_KEY));

        Map<String, Object> negative =
                GraphStatusAggregate.of(entries, -5).asMap();
        Assert.assertTrue(negative.containsKey(EXPECTED_COUNT_KEY));
        Assert.assertNull(negative.get(EXPECTED_COUNT_KEY));
    }

    @Test
    public void testServersOrderedByServerId() {
        List<GraphStatusEntry> entries =
                Arrays.asList(entry("server-c", GraphStatus.READY),
                              entry(null, GraphStatus.READY),
                              entry("server-a", GraphStatus.READY),
                              entry("server-b", GraphStatus.READY));

        GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries, 0);

        List<GraphStatusEntry> sorted = aggregate.entries();
        Assert.assertEquals(4, sorted.size());
        Assert.assertEquals("server-a", sorted.get(0).server());
        Assert.assertEquals("server-b", sorted.get(1).server());
        Assert.assertEquals("server-c", sorted.get(2).server());
        Assert.assertNull(sorted.get(3).server());

        List<Map<String, Object>> servers = servers(aggregate.asMap());
        Assert.assertEquals("server-a", servers.get(0).get(SERVER_KEY));
        Assert.assertEquals("server-b", servers.get(1).get(SERVER_KEY));
        Assert.assertEquals("server-c", servers.get(2).get(SERVER_KEY));
        Assert.assertNull(servers.get(3).get(SERVER_KEY));
    }

    @Test
    public void testStatusOfAServerThatIsGoneIsDropped() {
        // The server that reported failed is no longer part of the cluster,
        // its status must not hold the graph down
        List<GraphStatusEntry> entries = Arrays.asList(
                entry("server-a", GraphStatus.READY),
                entry("server-b", GraphStatus.FAILED));
        GraphStatusAggregate aggregate = GraphStatusAggregate.of(
                entries, Collections.singletonList("server-a"));

        Assert.assertEquals(GraphStatus.READY, aggregate.status());
        Assert.assertEquals(1, aggregate.readyCount());
        Assert.assertEquals(1, aggregate.totalCount());
        Assert.assertEquals(1, aggregate.expected());
    }

    @Test
    public void testReadyOfAServerThatIsGoneDoesNotCount() {
        // Two servers of an earlier deployment left a ready status behind,
        // they must not make up the quorum the running servers didn't reach
        List<GraphStatusEntry> entries = Arrays.asList(
                entry("old-a", GraphStatus.READY),
                entry("old-b", GraphStatus.READY),
                entry("server-a", GraphStatus.READY));
        GraphStatusAggregate aggregate = GraphStatusAggregate.of(
                entries, Arrays.asList("server-a", "server-b"));

        Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
        Assert.assertEquals(1, aggregate.readyCount());
        Assert.assertEquals(1, aggregate.totalCount());
        Assert.assertEquals(2, aggregate.expected());
    }

    @Test
    public void testAllServersGoneLeavesTheStatusUnknown() {
        List<GraphStatusEntry> entries = Collections.singletonList(
                entry("old-a", GraphStatus.FAILED));
        GraphStatusAggregate aggregate = GraphStatusAggregate.of(
                entries, Collections.singletonList("server-a"));

        Assert.assertNull(aggregate.status());
        Assert.assertEquals(0, aggregate.totalCount());
        Assert.assertEquals("UNKNOWN", aggregate.asMap().get(STATUS_KEY));
    }

    @Test
    public void testUnknownServersNeverReachReady() {
        // The servers of the graph space can't be listed, so the status that
        // was reported can't be told apart from the status of servers that
        // are gone and nothing is dropped
        List<GraphStatusEntry> entries = Collections.singletonList(
                entry("server-a", GraphStatus.READY));

        for (Collection<String> unknown : Arrays.asList(
                (Collection<String>) null, Collections.<String>emptyList())) {
            GraphStatusAggregate aggregate = GraphStatusAggregate.of(entries,
                                                                     unknown);
            Assert.assertEquals(GraphStatus.LOADING, aggregate.status());
            Assert.assertEquals(1, aggregate.totalCount());
            Assert.assertNull(aggregate.asMap().get(EXPECTED_COUNT_KEY));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> servers(Map<String, Object> map) {
        Object servers = map.get(SERVERS_KEY);
        Assert.assertInstanceOf(List.class, servers);
        return (List<Map<String, Object>>) servers;
    }

    private static GraphStatusEntry entry(String server, GraphStatus status) {
        return new GraphStatusEntry(server, status, null, 1L);
    }
}
