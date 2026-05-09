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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.ram.IntObjectMap;
import org.apache.hugegraph.backend.tx.SchemaTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;

import com.google.common.collect.ImmutableSet;

public final class CachedSchemaTransaction extends SchemaTransaction {

    /*
     * Listener lifetime must cover all active transactions for the graph.
     * The holder is removed from the registry and unregistered from EventHub
     * only when the last transaction releases it.
     */
    private static final ConcurrentMap<String, CacheListenerHolder>
            SCHEMA_CACHE_EVENT_LISTENERS = new ConcurrentHashMap<>();

    private final Cache<Id, Object> idCache;
    private final Cache<Id, Object> nameCache;

    private final SchemaCaches<SchemaElement> arrayCaches;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;
    private CacheListenerHolder holder;

    public CachedSchemaTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        final long capacity = graph.configuration()
                                   .get(CoreOptions.SCHEMA_CACHE_CAPACITY);
        this.idCache = this.cache("schema-id", capacity);
        this.nameCache = this.cache("schema-name", capacity);

        SchemaCaches<SchemaElement> attachment = this.idCache.attachment();
        if (attachment == null) {
            int acSize = (int) (capacity >> 3);
            attachment = this.idCache.attachment(new SchemaCaches<>(acSize));
        }
        this.arrayCaches = attachment;

        this.listenChanges();
    }

    private static Id generateId(HugeType type, Id id) {
        // NOTE: it's slower performance to use:
        // String.format("%x-%s", type.code(), name)
        return IdGenerator.of(type.string() + "-" + id.asString());
    }

    private static Id generateId(HugeType type, String name) {
        return IdGenerator.of(type.string() + "-" + name);
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.clearCache(false);
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix, long capacity) {
        final String name = prefix + "-" + this.graph().spaceGraphName();
        // NOTE: must disable schema cache-expire due to getAllSchema()
        return CacheManager.instance().cache(name, capacity);
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear schema cache on event '{}'",
                          this.graph(), event.name());
                this.clearCache(true);
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        EventListener listener = event -> {
            LOG.debug("Graph {} received schema cache event: {}",
                      this.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                            "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class, Id.class);
                HugeType type = (HugeType) args[1];
                Id id = (Id) args[2];
                this.invalidateCache(type, id);
                this.resetCachedAll(type);
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                this.clearCache(false);
                return true;
            }
            return false;
        };
        EventHub schemaEventHub = this.params().schemaEventHub();
        String graph = this.params().spaceGraphName();
        CacheListenerHolder acquired = SCHEMA_CACHE_EVENT_LISTENERS.compute(
                graph, (key, existing) -> {
                    if (existing == null || existing.hub != schemaEventHub) {
                        // Graph close/reopen creates a new EventHub for the
                        // same graph name; replace the stale holder. Old
                        // transactions skip decrement via identity check.
                        if (existing != null) {
                            existing.hub.unlisten(Events.CACHE,
                                                  existing.listener);
                        }
                        schemaEventHub.listen(Events.CACHE, listener);
                        return new CacheListenerHolder(listener,
                                                       schemaEventHub);
                    }
                    existing.refCount++;
                    return existing;
                });
        this.holder = acquired;
        this.cacheEventListener = acquired.listener;
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        CacheListenerHolder ours = this.holder;
        if (ours != null) {
            SCHEMA_CACHE_EVENT_LISTENERS.compute(
                    this.params().spaceGraphName(), (key, existing) -> {
                        if (existing == null || existing != ours) {
                            return existing;
                        }
                        existing.refCount--;
                        if (existing.refCount == 0) {
                            existing.hub.unlisten(Events.CACHE,
                                                  existing.listener);
                            return null;
                        }
                        return existing;
                    });
            this.holder = null;
            this.cacheEventListener = null;
        }
    }

    private void notifyChanges(String action, HugeType type, Id id) {
        EventHub graphEventHub = this.params().schemaEventHub();
        graphEventHub.notifyExcept(Events.CACHE, this.cacheEventListener,
                                   action, type, id);
    }

    private void notifyChanges(String action, HugeType type) {
        EventHub graphEventHub = this.params().schemaEventHub();
        graphEventHub.notifyExcept(Events.CACHE, this.cacheEventListener,
                                   action, type);
    }

    private void resetCachedAll(HugeType type) {
        // Set the cache all flag of the schema type to false
        this.cachedTypes().put(type, false);
    }

    private void resetCachedAllIfReachedCapacity() {
        if (this.idCache.size() >= this.idCache.capacity()) {
            LOG.warn("Schema cache reached capacity({}): {}",
                     this.idCache.capacity(), this.idCache.size());
            this.cachedTypes().clear();
        }
    }

    private CachedTypes cachedTypes() {
        return this.arrayCaches.cachedTypes();
    }

    public void clearCache(boolean notify) {
        this.idCache.clear();
        this.nameCache.clear();
        this.arrayCaches.clear();

        if (notify) {
            this.notifyChanges(Cache.ACTION_CLEAR, null);
        }
    }

    private void updateCache(SchemaElement schema) {
        this.resetCachedAllIfReachedCapacity();

        // update id cache
        Id prefixedId = generateId(schema.type(), schema.id());
        this.idCache.update(prefixedId, schema);

        // update name cache
        Id prefixedName = generateId(schema.type(), schema.name());
        this.nameCache.update(prefixedName, schema);

        // update optimized array cache
        this.arrayCaches.updateIfNeeded(schema);
    }

    private void invalidateCache(HugeType type, Id id) {
        // remove from id cache and name cache
        Id prefixedId = generateId(type, id);
        Object value = this.idCache.get(prefixedId);
        if (value != null) {
            this.idCache.invalidate(prefixedId);

            SchemaElement schema = (SchemaElement) value;
            Id prefixedName = generateId(schema.type(), schema.name());
            this.nameCache.invalidate(prefixedName);
        }

        // remove from optimized array cache
        this.arrayCaches.remove(type, id);
    }

    @Override
    protected void updateSchema(SchemaElement schema,
                                Consumer<SchemaElement> updateCallback) {
        super.updateSchema(schema, updateCallback);

        this.updateCache(schema);

        this.notifyChanges(Cache.ACTION_INVALID, schema.type(), schema.id());
    }

    @Override
    protected void addSchema(SchemaElement schema) {
        super.addSchema(schema);

        this.updateCache(schema);

        this.notifyChanges(Cache.ACTION_INVALID, schema.type(), schema.id());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        // try get from optimized array cache
        if (id.number() && id.asLong() > 0L) {
            SchemaElement value = this.arrayCaches.get(type, id);
            if (value != null) {
                return (T) value;
            }
        }

        Id prefixedId = generateId(type, id);
        Object value = this.idCache.get(prefixedId);
        if (value == null) {
            value = super.getSchema(type, id);
            if (value != null) {
                SchemaElement schema = (SchemaElement) value;
                // update id cache, name cache and optimized array cache
                this.updateCache(schema);
            }
        } else {
            // update optimized array cache for the result from id cache
            this.arrayCaches.updateIfNeeded((SchemaElement) value);
        }

        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type,
                                                    String name) {
        Id prefixedName = generateId(type, name);
        Object value = this.nameCache.get(prefixedName);
        if (value == null) {
            value = super.getSchema(type, name);
            if (value != null) {
                SchemaElement schema = (SchemaElement) value;
                this.updateCache(schema);
            }
        }
        return (T) value;
    }

    @Override
    public void removeSchema(SchemaElement schema) {
        super.removeSchema(schema);

        this.invalidateCache(schema.type(), schema.id());

        this.notifyChanges(Cache.ACTION_INVALID, schema.type(), schema.id());
    }

    @Override
    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        Boolean cachedAll = this.cachedTypes().getOrDefault(type, false);
        if (cachedAll) {
            List<T> results = new ArrayList<>();
            // Get from cache
            this.idCache.traverse(value -> {
                @SuppressWarnings("unchecked")
                T schema = (T) value;
                if (schema.type() == type) {
                    results.add(schema);
                }
            });
            return results;
        } else {
            this.cachedTypes().remove(type);
            List<T> results = super.getAllSchema(type);
            long free = this.idCache.capacity() - this.idCache.size();
            if (results.size() <= free) {
                // Update cache
                for (T schema : results) {
                    this.updateCache(schema);
                }
                this.cachedTypes().putIfAbsent(type, true);
            }
            return results;
        }
    }

    private static final class SchemaCaches<V extends SchemaElement> {

        private final int size;

        private final IntObjectMap<V> pks;
        private final IntObjectMap<V> vls;
        private final IntObjectMap<V> els;
        private final IntObjectMap<V> ils;

        private final CachedTypes cachedTypes;

        public SchemaCaches(int size) {
            // TODO: improve size of each type for optimized array cache
            this.size = size;

            this.pks = new IntObjectMap<>(size);
            this.vls = new IntObjectMap<>(size);
            this.els = new IntObjectMap<>(size);
            this.ils = new IntObjectMap<>(size);

            this.cachedTypes = new CachedTypes();
        }

        public void updateIfNeeded(V schema) {
            if (schema == null) {
                return;
            }
            Id id = schema.id();
            if (id.number() && id.asLong() > 0L) {
                this.set(schema.type(), id, schema);
            }
        }

        @Watched
        public V get(HugeType type, Id id) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                assert false : id;
                return null;
            }
            int key = (int) longId;
            if (key >= this.size) {
                return null;
            }
            switch (type) {
                case PROPERTY_KEY:
                    return this.pks.get(key);
                case VERTEX_LABEL:
                    return this.vls.get(key);
                case EDGE_LABEL:
                    return this.els.get(key);
                case INDEX_LABEL:
                    return this.ils.get(key);
                default:
                    return null;
            }
        }

        public void set(HugeType type, Id id, V value) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                assert false : id;
                return;
            }
            int key = (int) longId;
            if (key >= this.size) {
                return;
            }

            this.setValue(type, key, value);
        }

        public void remove(HugeType type, Id id) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                return;
            }
            int key = (int) longId;
            if (key >= this.size) {
                return;
            }

            this.setValue(type, key, null);
        }

        public void clear() {
            this.pks.clear();
            this.vls.clear();
            this.els.clear();
            this.ils.clear();

            this.cachedTypes.clear();
        }

        public CachedTypes cachedTypes() {
            return this.cachedTypes;
        }

        private void setValue(HugeType type, int key, V value) {
            switch (type) {
                case PROPERTY_KEY:
                    this.pks.set(key, value);
                    break;
                case VERTEX_LABEL:
                    this.vls.set(key, value);
                    break;
                case EDGE_LABEL:
                    this.els.set(key, value);
                    break;
                case INDEX_LABEL:
                    this.ils.set(key, value);
                    break;
                default:
                    // pass
                    break;
            }
        }
    }

    private static class CachedTypes
            extends ConcurrentHashMap<HugeType, Boolean> {

        private static final long serialVersionUID = -2215549791679355996L;
    }
}
