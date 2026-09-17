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

package org.apache.hugegraph.pd.metrics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.model.GraphStatistics;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public final class PDMetrics {

    public final static String PREFIX = "hg";
    private static AtomicLong graphs = new AtomicLong(0);
    private static Map<String, Counter> lastTerms = new ConcurrentHashMap();
    @Autowired
    PDRestService pdRestService;
    @Autowired
    private PDService pdService;
    private MeterRegistry registry;
    private Map<String, Pair<Long, Long>> lasts = new ConcurrentHashMap();
    private int interval = 120 * 1000;

    public synchronized void init(MeterRegistry meterRegistry) {

        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }

    }

    private void registerMeters() {
        Gauge.builder(PREFIX + ".up", () -> 1).register(registry);
        Gauge.builder(PREFIX + ".graphs", () -> updateGraphs())
             .description("Number of graphs registered in PD")
             .register(registry);
        Gauge.builder(PREFIX + ".stores", () -> updateStores())
             .description("Number of stores registered in PD")
             .register(registry);
        Gauge.builder(PREFIX + ".terms", () -> setTerms())
             .description("term of partitions in PD")
             .register(registry);
        registerRaftMeters();
    }

    /**
     * Raft membership gauges so operators can alert on quorum loss. They mirror what
     * {@code GET /v1/ready} answers: a PD that sees no leader is outside a quorum.
     */
    private void registerRaftMeters() {
        RaftEngine raft = RaftEngine.getInstance();
        Gauge.builder(PREFIX + ".raft.leader", () -> raft.getRaftStatus().isLocalLeader() ? 1 : 0)
             .description("1 if this PD is the raft leader, 0 otherwise")
             .register(registry);
        Gauge.builder(PREFIX + ".raft.has.leader", () -> raft.hasLeader() ? 1 : 0)
             .description("1 if this PD sees a raft leader, i.e. is part of a quorum, 0 otherwise")
             .register(registry);
        Gauge.builder(PREFIX + ".raft.alive.peers", () -> {
                 int alive = raft.getAlivePeerCount();
                 return alive < 0 ? Double.NaN : alive;
             })
             .description("Number of raft peers, itself included, the leader has heard from " +
                          "within the leader lease timeout; NaN on non-leader nodes")
             .register(registry);
    }

    private long updateGraphs() {
        long buf = getGraphs();
        if (buf != graphs.get()) {
            graphs.set(buf);
            registerGraphMetrics();
        }
        return buf;
    }

    private long updateStores() {
        return getStores();
    }

    private long getGraphs() {
        return getGraphMetas().size();
    }

    private long getStores() {
        try {
            return this.pdService.getStoreNodeService().getStores(null).size();
        } catch (PDException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return 0;
    }

    private long setTerms() {
        List<ShardGroup> groups = null;
        try {
            groups = pdRestService.getShardGroups();
            StoreNodeService nodeService = pdService.getStoreNodeService();
            for (ShardGroup g : groups) {
                String id = String.valueOf(g.getId());
                ShardGroup group = nodeService.getShardGroup(g.getId());
                long version = group.getVersion();
                Counter lastTerm = lastTerms.get(id);
                if (lastTerm == null) {
                    lastTerm = Counter.builder(PREFIX + ".partition.terms")
                                      .description("term of partition")
                                      .tag("id", id)
                                      .register(this.registry);
                    lastTerm.increment(version);
                    lastTerms.put(id, lastTerm);
                } else {
                    lastTerm.increment(version - lastTerm.count());
                }
            }
        } catch (Exception e) {
            log.info("get partition term with error :", e);
        }
        if (groups == null) {
            return 0;
        } else {
            return groups.size();
        }
    }

    private List<Metapb.Graph> getGraphMetas() {
        try {
            return this.pdService.getPartitionService().getGraphs();
        } catch (PDException e) {
            log.error(e.getMessage(), e);
        }
        return Collections.EMPTY_LIST;
    }

    private void registerGraphMetrics() {
        this.getGraphMetas().forEach(meta -> {
            Gauge.builder(PREFIX + ".partitions", this.pdService.getPartitionService()
                         , e -> e.getPartitions(meta.getGraphName()).size())
                 .description("Number of partitions assigned to a graph")
                 .tag("graph", meta.getGraphName())
                 .register(this.registry);
            ToDoubleFunction<Metapb.Graph> getGraphSize = e -> {
                try {
                    String graphName = e.getGraphName();
                    Pair<Long, Long> last = lasts.get(graphName);
                    Long lastTime;
                    if (last == null || (lastTime = last.getLeft()) == null ||
                        System.currentTimeMillis() - lastTime >= interval) {
                        long dataSize =
                                new GraphStatistics(e, pdRestService, pdService).getDataSize();
                        lasts.put(graphName, Pair.of(System.currentTimeMillis(), dataSize));
                        return dataSize;
                    } else {
                        return last.getRight();
                    }
                } catch (PDException ex) {
                    log.error("get graph size with error", e);
                }
                return 0;
            };
            Gauge.builder(PREFIX + ".graph.size", meta, getGraphSize)
                 .description("data size of graph")
                 .tag("graph", meta.getGraphName())
                 .register(this.registry);
        });
    }

}
