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

package org.apache.hugegraph.backend.cache;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.CachedBackendStore.QueryId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.ram.RamTable;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.iterator.ListIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;

import com.google.common.collect.ImmutableSet;

public final class CachedGraphTransaction extends GraphTransaction {

    private static final int MAX_CACHE_PROPS_PER_VERTEX = 10000;
    private static final int MAX_CACHE_EDGES_PER_QUERY = 100;
    private static final float DEFAULT_LEVEL_RATIO = 0.001f;
    private static final long AVG_VERTEX_ENTRY_SIZE = 40L;
    private static final long AVG_EDGE_ENTRY_SIZE = 100L;

    /*
     * Listener lifetime must cover all active transactions for the graph.
     * The holder is removed from the registry and unregistered from EventHub
     * only when the last transaction releases it.
     */
    private static final ConcurrentMap<String, CacheListenerHolder>
            GRAPH_CACHE_EVENT_LISTENERS = new ConcurrentHashMap<>();

    private final Cache<Id, Object> verticesCache;
    private final Cache<Id, Object> edgesCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;
    private CacheListenerHolder holder;

    public CachedGraphTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        HugeConfig conf = graph.configuration();

        String type = conf.get(CoreOptions.VERTEX_CACHE_TYPE);
        long capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", type, capacity,
                                        AVG_VERTEX_ENTRY_SIZE, expire);

        type = conf.get(CoreOptions.EDGE_CACHE_TYPE);
        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", type, capacity,
                                     AVG_EDGE_ENTRY_SIZE, expire);

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix, String type, long capacity,
                                    long entrySize, long expire) {
        String name = prefix + "-" + this.params().spaceGraphName();
        Cache<Id, Object> cache;
        switch (type) {
            case "l1":
                cache = CacheManager.instance().cache(name, capacity);
                break;
            case "l2":
                long heapCapacity = (long) (DEFAULT_LEVEL_RATIO * capacity);
                cache = CacheManager.instance().levelCache(super.graph(),
                                                           name, heapCapacity,
                                                           capacity, entrySize);
                break;
            default:
                throw new NotSupportException("cache type '%s'", type);
        }
        // Convert the unit from seconds to milliseconds
        cache.expire(expire * 1000L);
        // Enable metrics for graph cache by default
        cache.enableMetrics(true);
        return cache;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                          this.graph(), event.name());
                this.clearCache(null, true);
                return true;
            }
            return false;
        };
        if (storeEventListenStatus.putIfAbsent(this.params().spaceGraphName(), true) == null) {
            this.store().provider().listen(this.storeEventListener);
        }

        // Listen cache event: "cache"(invalid cache item)
        EventListener listener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                      this.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                            "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class, Object.class);
                HugeType type = (HugeType) args[1];
                if (type.isVertex()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.verticesCache.invalidate(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                            "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.verticesCache.invalidate((Id) id);
                        }
                    } else {
                        E.checkArgument(false,
                                        "Expect Id or Id[], but got: %s",
                                        arg2);
                    }
                } else if (type.isEdge()) {
                    /*
                     * Invalidate edge cache via clear instead of invalidate
                     * because of the cacheKey is QueryId not EdgeId
                     */
                    // this.edgesCache.invalidate(id);
                    this.edgesCache.clear();
                }
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                HugeType type = (HugeType) args[1];
                this.clearCache(type, false);
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.params().graphEventHub();
        String graphName = this.params().spaceGraphName();
        CacheListenerHolder acquired = GRAPH_CACHE_EVENT_LISTENERS.compute(
                graphName, (key, existing) -> {
                    if (existing == null || existing.hub != graphEventHub) {
                        // Graph close/reopen creates a new EventHub for the
                        // same graph name; replace the stale holder. Old
                        // transactions skip decrement via identity check.
                        if (existing != null) {
                            existing.hub.unlisten(Events.CACHE,
                                                  existing.listener);
                        }
                        graphEventHub.listen(Events.CACHE, listener);
                        return new CacheListenerHolder(listener, graphEventHub);
                    }
                    existing.refCount++;
                    return existing;
                });
        this.holder = acquired;
        this.cacheEventListener = acquired.listener;
    }

    private void unlistenChanges() {
        String graphName = this.params().spaceGraphName();
        CacheListenerHolder ours = this.holder;
        if (ours != null) {
            GRAPH_CACHE_EVENT_LISTENERS.compute(graphName, (key, existing) -> {
                if (existing == null || existing != ours) {
                    return existing;
                }
                existing.refCount--;
                if (existing.refCount == 0) {
                    existing.hub.unlisten(Events.CACHE, existing.listener);
                    return null;
                }
                return existing;
            });
            this.holder = null;
            this.cacheEventListener = null;
        }
        // TODO (follow-up): storeEventListenStatus has the same owner-first
        // close bug this PR fixes for GRAPH_CACHE_EVENT_LISTENERS. A non-owner
        // transaction can remove the tracking entry, unlisten its own
        // never-registered storeEventListener as a no-op, and leave the
        // original store listener registered but untracked. Apply the same
        // ref-counted holder pattern in a follow-up PR.
        if (storeEventListenStatus.remove(graphName) != null) {
            this.store().provider().unlisten(this.storeEventListener);
        }
    }

    private void notifyChanges(String action, HugeType type, Id[] ids) {
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.notifyExcept(Events.CACHE, this.cacheEventListener,
                                   action, type, ids);
    }

    private void notifyChanges(String action, HugeType type) {
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.notifyExcept(Events.CACHE, this.cacheEventListener,
                                   action, type);
    }

    public void clearCache(HugeType type, boolean notify) {
        if (type == null || type == HugeType.VERTEX) {
            this.verticesCache.clear();
        }
        if (type == null || type == HugeType.EDGE) {
            this.edgesCache.clear();
        }

        if (notify) {
            this.notifyChanges(Cache.ACTION_CLEAR, null);
        }
    }

    private boolean enableCacheVertex() {
        return this.verticesCache.capacity() > 0L;
    }

    private boolean enableCacheEdge() {
        return this.edgesCache.capacity() > 0L;
    }

    private boolean needCacheVertex(HugeVertex vertex) {
        return vertex.sizeOfSubProperties() <= MAX_CACHE_PROPS_PER_VERTEX;
    }

    @Override
    @Watched(prefix = "graphcache")
    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        if (this.enableCacheVertex() &&
            query.idsSize() > 0 && query.conditionsSize() == 0) {
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    @Watched(prefix = "graphcache")
    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query) {
        if (query.idsSize() == 1) {
            Id vertexId = query.ids().iterator().next();
            HugeVertex vertex = (HugeVertex) this.verticesCache.get(vertexId);
            if (vertex != null) {
                if (!vertex.expired()) {
                    return QueryResults.iterator(vertex);
                }
                this.verticesCache.invalidate(vertexId);
            }
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(query);
            vertex = QueryResults.one(rs);
            if (vertex == null) {
                return QueryResults.emptyIterator();
            }
            if (needCacheVertex(vertex)) {
                this.verticesCache.update(vertex.id(), vertex);
            }
            return QueryResults.iterator(vertex);
        }

        IdQuery newQuery = new IdQuery(HugeType.VERTEX, query);
        List<HugeVertex> vertices = new ArrayList<>();
        for (Id vertexId : query.ids()) {
            HugeVertex vertex = (HugeVertex) this.verticesCache.get(vertexId);
            if (vertex == null) {
                newQuery.query(vertexId);
            } else if (vertex.expired()) {
                newQuery.query(vertexId);
                this.verticesCache.invalidate(vertexId);
            } else {
                vertices.add(vertex);
            }
        }

        // Join results from cache and backend
        ExtendableIterator<HugeVertex> results = new ExtendableIterator<>();
        if (!vertices.isEmpty()) {
            results.extend(vertices.iterator());
        } else {
            // Just use the origin query if find none from the cache
            newQuery = query;
        }

        if (!newQuery.empty()) {
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(newQuery);
            // Generally there are not too much data with id query
            ListIterator<HugeVertex> listIterator = QueryResults.toList(rs);
            for (HugeVertex vertex : listIterator.list()) {
                // Skip large vertex
                if (needCacheVertex(vertex)) {
                    this.verticesCache.update(vertex.id(), vertex);
                }
            }
            results.extend(listIterator);
        }

        return results;
    }

    @Override
    @Watched(prefix = "graphcache")
    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        RamTable ramtable = this.params().ramtable();
        if (ramtable != null && ramtable.matched(query)) {
            return ramtable.query(query);
        }

        if (!this.enableCacheEdge() || query.empty() ||
            query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        Id cacheKey = new QueryId(query);
        Object value = this.edgesCache.get(cacheKey);
        @SuppressWarnings("unchecked")
        Collection<HugeEdge> edges = (Collection<HugeEdge>) value;
        if (value != null) {
            for (HugeEdge edge : edges) {
                if (edge.expired()) {
                    this.edgesCache.invalidate(cacheKey);
                    value = null;
                    break;
                }
            }
        }

        if (value != null) {
            // Not cached or the cache expired
            return edges.iterator();
        }

        Iterator<HugeEdge> rs = super.queryEdgesFromBackend(query);

        /*
         * Iterator can't be cached, caching list instead
         * there may be super node and too many edges in a query,
         * try fetch a few of the head results and determine whether to cache.
         */
        final int tryMax = 1 + MAX_CACHE_EDGES_PER_QUERY;
        edges = new ArrayList<>(tryMax);
        for (int i = 0; rs.hasNext() && i < tryMax; i++) {
            edges.add(rs.next());
        }

        if (edges.isEmpty()) {
            this.edgesCache.update(cacheKey, Collections.emptyList());
        } else if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
            this.edgesCache.update(cacheKey, edges);
        }

        return new ExtendableIterator<>(edges.iterator(), rs);
    }

    @Override
    @Watched(prefix = "graphcache")
    protected void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        Collection<HugeVertex> updates = this.verticesInTxUpdated();
        Collection<HugeVertex> deletions = this.verticesInTxRemoved();
        Id[] vertexIds = new Id[updates.size() + deletions.size()];
        int vertexOffset = 0;

        int edgesInTxSize = this.edgesInTxSize();

        try {
            super.commitMutation2Backend(mutations);
            // Update vertex cache
            if (this.enableCacheVertex()) {
                for (HugeVertex vertex : updates) {
                    vertexIds[vertexOffset++] = vertex.id();
                    if (needCacheVertex(vertex)) {
                        // Update cache
                        this.verticesCache.updateIfPresent(vertex.id(), vertex);
                    } else {
                        // Skip large vertex
                        this.verticesCache.invalidate(vertex.id());
                    }
                }
            }
        } finally {
            // Update removed vertex in cache whatever success or fail
            if (this.enableCacheVertex()) {
                for (HugeVertex vertex : deletions) {
                    vertexIds[vertexOffset++] = vertex.id();
                    this.verticesCache.invalidate(vertex.id());
                }
                if (vertexOffset > 0) {
                    this.notifyChanges(Cache.ACTION_INVALID,
                                       HugeType.VERTEX, vertexIds);
                }
            }

            /*
             * Update edge cache if any vertex or edge changed
             * For vertex change, the edges linked with should also be updated
             * Before we find a more precise strategy, just clear all the edge cache now
             */
            boolean invalidEdgesCache = (edgesInTxSize + updates.size() + deletions.size()) > 0;
            if (invalidEdgesCache && this.enableCacheEdge()) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
                this.notifyChanges(Cache.ACTION_CLEAR, HugeType.EDGE);
            }
        }
    }

    @Override
    public void removeIndex(IndexLabel indexLabel) {
        try {
            super.removeIndex(indexLabel);
        } finally {
            // Update edge cache if needed (any edge-index is deleted)
            if (indexLabel.baseType() == HugeType.EDGE_LABEL) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
                this.notifyChanges(Cache.ACTION_CLEAR, HugeType.EDGE);
            }
        }
    }
}
