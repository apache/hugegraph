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

package org.apache.hugegraph.store.node.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.alipay.sofa.jraft.core.NodeMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HgStoreMetricsControllerTest {

    private HgStoreMetricsController controller;
    private HgStoreNodeService service;
    private MockMvc mvc;

    @BeforeEach
    public void setUp() {
        this.controller = new HgStoreMetricsController();
        this.service = mock(HgStoreNodeService.class);
        this.controller.nodeService = this.service;
        this.mvc = MockMvcBuilders.standaloneSetup(this.controller).build();
    }

    @Test
    public void testSummaryCountsEnabledAndDisabledGroups() throws Exception {
        Map<String, NodeMetrics> groups = new HashMap<>();
        groups.put("1", new NodeMetrics(true));
        groups.put("2", new NodeMetrics(false));
        groups.put("3", null);
        when(this.service.getNodeMetrics()).thenReturn(groups);

        String json = this.mvc.perform(get("/metrics/raft/summary"))
                              .andExpect(status().isOk())
                              .andReturn().getResponse().getContentAsString();
        assertEquals(Map.of("groups", 3, "enabled_groups", 1),
                     new ObjectMapper().readValue(json, Map.class));
    }

    @Test
    public void testEmptySummaryIsAvailable() {
        when(this.service.getNodeMetrics()).thenReturn(Map.of());
        assertEquals(Map.of("groups", 0L, "enabled_groups", 0L),
                     this.controller.getRaftMetricsSummary());
    }

    @Test
    public void testLargeRegistriesAreNotSerializedBySummary() throws Exception {
        Map<String, NodeMetrics> groups = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            NodeMetrics metrics = new NodeMetrics(true);
            // Full registries contain timer distributions and samples for every
            // group. The summary must never traverse or serialize them.
            metrics.recordLatency("append-entries", i);
            metrics.recordTimes("replicator-events", i);
            groups.put(Integer.toString(i), metrics);
        }
        when(this.service.getNodeMetrics()).thenReturn(groups);

        String json = this.mvc.perform(get("/metrics/raft/summary"))
                              .andExpect(status().isOk())
                              .andReturn().getResponse().getContentAsString();
        assertTrue(json.getBytes(StandardCharsets.UTF_8).length < 100);
        assertEquals(Map.of("groups", 1000, "enabled_groups", 1000),
                     new ObjectMapper().readValue(json, Map.class));
    }

    @Test
    public void testDetailedEndpointPreservesItsContract() throws Exception {
        Map<String, NodeMetrics> groups = Map.of("partition-1", new NodeMetrics(true));
        when(this.service.getNodeMetrics()).thenReturn(groups);
        this.mvc.perform(get("/metrics/raft"))
                .andExpect(status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.content()
                        .json(new ObjectMapper().writeValueAsString(groups)));
    }
}
