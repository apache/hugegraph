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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class StoreStateMetricsTest {

    private SimpleMeterRegistry registry;
    private List<Metapb.Store> stores;
    private List<Metapb.ShardGroup> groups;
    private boolean failRead;
    private StoreStateMetrics metrics;

    @Before
    public void setup() {
        this.registry = new SimpleMeterRegistry();
        this.stores = new ArrayList<>();
        this.groups = new ArrayList<>();
        this.failRead = false;
        this.metrics = new StoreStateMetrics(this.registry, () -> {
            if (this.failRead) {
                throw new PDException(-1, "raft is not ready");
            }
            return this.stores;
        }, () -> this.groups);
    }

    @Test
    public void testStateAndLeaderCount() {
        this.stores.add(store(1, Metapb.StoreState.Up));
        this.stores.add(store(2, Metapb.StoreState.Up));
        this.stores.add(store(3, Metapb.StoreState.Offline));
        // store 3 went down, so both groups elected leaders on store 1
        this.groups.add(group(1, 1, 2, 3));
        this.groups.add(group(2, 1, 2, 3));
        this.metrics.refresh();

        Assert.assertEquals(1.0, state(1, "Up"), 0);
        Assert.assertEquals(0.0, state(1, "Offline"), 0);
        Assert.assertEquals(1.0, state(3, "Offline"), 0);
        Assert.assertEquals(0.0, state(3, "Up"), 0);
        Assert.assertEquals(2.0, leaders(1), 0);
        Assert.assertEquals(0.0, leaders(2), 0);
        Assert.assertEquals(0.0, leaders(3), 0);
        // One gauge per state except UNRECOGNIZED, for each store
        int states = Metapb.StoreState.values().length - 1;
        Assert.assertEquals(3 * states,
                            this.registry.find("hg.store.state").gauges().size());
    }

    @Test
    public void testStoresJoinAndLeave() {
        this.stores.add(store(1, Metapb.StoreState.Up));
        this.metrics.refresh();
        Assert.assertNull(gauge("hg.store.leader.count", 2));

        this.stores.add(store(2, Metapb.StoreState.Pending));
        this.metrics.refresh();
        Assert.assertEquals(1.0, state(2, "Pending"), 0);

        this.stores.remove(0);
        this.metrics.refresh();
        Assert.assertNull(gauge("hg.store.leader.count", 1));
        Assert.assertTrue(this.registry.find("hg.store.state").tag("store", "1")
                                       .gauges().isEmpty());
        Assert.assertNotNull(gauge("hg.store.leader.count", 2));
    }

    @Test
    public void testReadErrorKeepsLastValues() {
        this.stores.add(store(1, Metapb.StoreState.Up));
        this.metrics.refresh();

        this.failRead = true;
        this.stores.clear();
        this.metrics.refresh();
        Assert.assertEquals(1.0, state(1, "Up"), 0);
    }

    @Test
    public void testPrometheusScrape() {
        var prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        var reads = new AtomicInteger();
        var scraped = new StoreStateMetrics(prometheus, () -> {
            reads.incrementAndGet();
            return this.stores;
        }, () -> this.groups);
        // As hg.stores in PDMetrics: the refresh registers meters while a scrape runs
        Gauge.builder("hg.stores", scraped::storeCount).register(prometheus);
        this.stores.add(store(7, Metapb.StoreState.Up));
        this.groups.add(group(1, 7));

        prometheus.scrape();
        String body = prometheus.scrape();
        // Both scrapes fall in one refresh interval, and hg.stores reuses the refreshed list
        Assert.assertEquals(1, reads.get());
        Assert.assertTrue(body, body.contains("hg_stores 1.0"));
        Assert.assertTrue(body, body.contains("hg_store_state{state=\"Up\",store=\"7\",} 1.0"));
        Assert.assertTrue(body,
                          body.contains("hg_store_state{state=\"Offline\",store=\"7\",} 0.0"));
        Assert.assertTrue(body, body.contains("hg_store_leader_count{store=\"7\",} 1.0"));
    }

    private double state(long storeId, String state) {
        return this.registry.get("hg.store.state")
                            .tag("store", String.valueOf(storeId))
                            .tag("state", state)
                            .gauge().value();
    }

    private double leaders(long storeId) {
        return gauge("hg.store.leader.count", storeId).value();
    }

    private Gauge gauge(String name, long storeId) {
        return this.registry.find(name).tag("store", String.valueOf(storeId)).gauge();
    }

    private static Metapb.Store store(long id, Metapb.StoreState state) {
        return Metapb.Store.newBuilder().setId(id).setState(state).build();
    }

    private static Metapb.ShardGroup group(int id, long leader, long... followers) {
        var builder = Metapb.ShardGroup.newBuilder().setId(id);
        builder.addShards(Metapb.Shard.newBuilder().setStoreId(leader)
                                      .setRole(Metapb.ShardRole.Leader));
        for (long follower : followers) {
            if (follower != leader) {
                builder.addShards(Metapb.Shard.newBuilder().setStoreId(follower)
                                              .setRole(Metapb.ShardRole.Follower));
            }
        }
        return builder.build();
    }
}
