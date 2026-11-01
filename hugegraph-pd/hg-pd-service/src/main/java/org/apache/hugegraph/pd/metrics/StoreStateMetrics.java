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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-store gauges built from the store and shard group metadata PD already keeps:
 * {@code hg.store.state{store,state}} is 1 for the store's current state and 0 for the
 * others, and {@code hg.store.leader.count{store}} counts the shard groups it leads.
 * Stores are registered and removed at runtime, so the meters follow the store list on
 * every refresh. The gauges, and {@code hg.stores} through {@link #storeCount()}, trigger
 * the refresh themselves at most once per {@link #REFRESH_INTERVAL_MS}, so one scrape reads
 * the metadata once.
 */
@Slf4j
final class StoreStateMetrics {

    static final long REFRESH_INTERVAL_MS = 1000;

    @FunctionalInterface
    interface Source<T> {

        T get() throws PDException;
    }

    private final MeterRegistry registry;
    private final Source<List<Metapb.Store>> stores;
    private final Source<List<Metapb.ShardGroup>> shardGroups;
    // Guarded by this
    private final Map<Long, List<Meter>> meters = new HashMap<>();
    private long lastRefresh;

    private volatile Map<Long, Metapb.StoreState> states = Collections.emptyMap();
    private volatile Map<Long, Integer> leaders = Collections.emptyMap();

    StoreStateMetrics(MeterRegistry registry, Source<List<Metapb.Store>> stores,
                      Source<List<Metapb.ShardGroup>> shardGroups) {
        this.registry = registry;
        this.stores = stores;
        this.shardGroups = shardGroups;
    }

    /**
     * Reads the store list and shard groups, registers meters for new stores and removes
     * those of stores PD no longer knows. On a read error the previous values are kept.
     */
    synchronized void refresh() {
        List<Metapb.Store> storeList;
        List<Metapb.ShardGroup> groups;
        try {
            storeList = this.stores.get();
            groups = this.shardGroups.get();
        } catch (PDException | RuntimeException e) {
            // RuntimeException: a scrape can arrive before PDService finishes init
            log.warn("Failed to read stores for store state metrics", e);
            return;
        }
        this.lastRefresh = System.currentTimeMillis();

        Map<Long, Metapb.StoreState> newStates = new HashMap<>();
        Map<Long, Integer> newLeaders = new HashMap<>();
        for (Metapb.Store store : storeList) {
            newStates.put(store.getId(), store.getState());
            newLeaders.put(store.getId(), 0);
        }
        for (Metapb.ShardGroup group : groups) {
            for (Metapb.Shard shard : group.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader &&
                    newLeaders.containsKey(shard.getStoreId())) {
                    newLeaders.merge(shard.getStoreId(), 1, Integer::sum);
                }
            }
        }
        this.states = newStates;
        this.leaders = newLeaders;

        for (Long storeId : newStates.keySet()) {
            this.meters.computeIfAbsent(storeId, this::registerStore);
        }
        Iterator<Map.Entry<Long, List<Meter>>> iter = this.meters.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, List<Meter>> entry = iter.next();
            if (!newStates.containsKey(entry.getKey())) {
                entry.getValue().forEach(this.registry::remove);
                iter.remove();
            }
        }
    }

    synchronized void refreshIfStale() {
        if (System.currentTimeMillis() - this.lastRefresh >= REFRESH_INTERVAL_MS) {
            this.refresh();
        }
    }

    private double state(long storeId, Metapb.StoreState state) {
        this.refreshIfStale();
        return this.states.get(storeId) == state ? 1 : 0;
    }

    /**
     * The number of stores PD had at the last refresh, which hg.stores reports so a scrape
     * reads the store list once
     */
    long storeCount() {
        this.refreshIfStale();
        return this.states.size();
    }

    private double leaderCount(long storeId) {
        this.refreshIfStale();
        Integer count = this.leaders.get(storeId);
        return count == null ? Double.NaN : count;
    }

    private List<Meter> registerStore(long storeId) {
        String store = String.valueOf(storeId);
        List<Meter> list = new ArrayList<>();
        for (Metapb.StoreState state : Metapb.StoreState.values()) {
            if (state == Metapb.StoreState.UNRECOGNIZED) {
                continue;
            }
            list.add(Gauge.builder(PDMetrics.PREFIX + ".store.state", this,
                                   m -> m.state(storeId, state))
                          .description("1 if the store is in this state, 0 otherwise")
                          .tag("store", store)
                          .tag("state", state.name())
                          .register(this.registry));
        }
        list.add(Gauge.builder(PDMetrics.PREFIX + ".store.leader.count", this,
                               m -> m.leaderCount(storeId))
                      .description("Number of shard groups whose leader is on the store")
                      .tag("store", store)
                      .register(this.registry));
        return list;
    }
}
