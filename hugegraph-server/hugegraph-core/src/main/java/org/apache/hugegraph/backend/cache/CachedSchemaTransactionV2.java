/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.ram.IntObjectMap;
import org.apache.hugegraph.backend.tx.SchemaTransactionV2;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.MetaManager.SchemaCacheClearEvent;
import org.apache.hugegraph.perf.PerfUtil;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;

import com.google.common.collect.ImmutableSet;

public class CachedSchemaTransactionV2 extends SchemaTransactionV2 {

    private static final String ID_CACHE_PREFIX = "schema-id";
    private static final String NAME_CACHE_PREFIX = "schema-name";

    // MetaDriver doesn't expose unlisten, register the meta listener once.
    // Lifecycle: this JVM-global flag is intentionally never reset by
    // unlistenChanges() (the underlying gRPC watch is process-wide). If that
    // watch is silently dropped after a transport reconnect, recovery is not
    // automatic; resetMetaListenerForReconnect() is only a manual hook to let
    // the next schema operation install a fresh watch.
    private static final AtomicBoolean metaEventListenerRegistered =
            new AtomicBoolean(false);

    private static final Object META_LISTENER_LOCK = new Object();

    /**
     * Per-JVM identifier emitted with every schema-cache-clear meta event so
     * the listener can skip its own echo. Lifecycle: generated once per
     * classloader at class init, never reused, regenerated on JVM restart.
     * This is not a stable node identity, only a local self-echo filter.
     */
    private static final String SCHEMA_CACHE_CLEAR_SOURCE =
            UUID.randomUUID().toString();

    private final Cache<Id, Object> idCache;
    private final Cache<Id, Object> nameCache;

    private final SchemaCaches<SchemaElement> arrayCaches;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    public CachedSchemaTransactionV2(MetaDriver metaDriver,
                                     String cluster,
                                     HugeGraphParams graphParams) {
        super(metaDriver, cluster, graphParams);

        final long capacity = graphParams.configuration()
                                         .get(CoreOptions.SCHEMA_CACHE_CAPACITY);
        this.idCache = this.cache(ID_CACHE_PREFIX, capacity);
        this.nameCache = this.cache(NAME_CACHE_PREFIX, capacity);

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

    public void close() {
        this.clearCache(false);
        this.unlistenChanges();
    }

    private Cache<Id, Object> cache(String prefix, long capacity) {
        final String name = cacheName(prefix, this.graph().spaceGraphName());
        // NOTE: must disable schema cache-expire due to getAllSchema()
        return CacheManager.instance().cache(name, capacity);
    }

    private static String cacheName(String prefix, String spaceGraphName) {
        return prefix + "-" + spaceGraphName;
    }

    private static void clearSchemaCache(String spaceGraphName) {
        Map<String, Cache<Id, Object>> caches = CacheManager.instance().caches();

        // Clear name cache first so the (name -> id -> object) lookup path
        // fails fast instead of returning a stale object backed by an
        // already-empty id cache during the TOCTOU window.
        Cache<Id, Object> nameCache = caches.get(cacheName(NAME_CACHE_PREFIX,
                                                           spaceGraphName));
        if (nameCache != null) {
            nameCache.clear();
        }

        Cache<Id, Object> idCache = caches.get(cacheName(ID_CACHE_PREFIX,
                                                         spaceGraphName));
        if (idCache != null) {
            SchemaCaches<?> arrayCaches = idCache.attachment();
            if (arrayCaches != null) {
                arrayCaches.clear();
            }
            idCache.clear();
        }
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
                boolean notify = !Events.STORE_INIT.equals(event.name());
                this.clearCache(notify);
                return true;
            }
            return false;
        };
        this.graphParams().loadGraphStore().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received schema cache event: {}",
                      this.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                            "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class, Id.class);
                HugeType type = (HugeType) args[1];
                Id id = (Id) args[2];
                this.arrayCaches.remove(type, id);

                id = generateId(type, id);
                Object value = this.idCache.get(id);
                if (value != null) {
                    // Invalidate id cache
                    this.idCache.invalidate(id);

                    // Invalidate name cache
                    SchemaElement schema = (SchemaElement) value;
                    Id prefixedName = generateId(schema.type(),
                                                 schema.name());
                    this.nameCache.invalidate(prefixedName);
                }
                this.resetCachedAll(type);
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                this.clearCache(false);
                return true;
            }
            return false;
        };
        EventHub schemaEventHub = this.graphParams().schemaEventHub();
        if (!schemaEventHub.containsListener(Events.CACHE)) {
            schemaEventHub.listen(Events.CACHE, this.cacheEventListener);
        }

        listenSchemaCacheClear();
    }

    private static void listenSchemaCacheClear() {
        synchronized (META_LISTENER_LOCK) {
            if (metaEventListenerRegistered.get()) {
                return;
            }
            try {
                MetaManager.instance().listenSchemaCacheClear(
                        CachedSchemaTransactionV2::handleSchemaCacheClearEvent);
                // Set AFTER the underlying watch is live so a concurrent
                // caller that observes the flag is guaranteed an active
                // subscription, and a failure leaves the flag false so the
                // next caller retries registration.
                metaEventListenerRegistered.set(true);
            } catch (Exception e) {
                throw e instanceof RuntimeException
                      ? (RuntimeException) e
                      : new RuntimeException(
                              "Failed to register schema cache clear listener",
                              e);
            }
        }
    }

    /**
     * Consumer invoked by the MetaManager schema-cache-clear watch. Extracted
     * as a package-private static method so end-to-end tests can drive the
     * publish -> callback -> {@link #clearSchemaCache(String)} path without
     * depending on a live etcd/PD watch.
     */
    static <T> void handleSchemaCacheClearEvent(T response) {
        List<SchemaCacheClearEvent> events =
                MetaManager.instance()
                           .extractSchemaCacheClearEventsFromResponse(
                                   response);
        if (events == null) {
            return;
        }
        for (SchemaCacheClearEvent event : events) {
            if (SCHEMA_CACHE_CLEAR_SOURCE.equals(event.source())) {
                continue;
            }
            String graphName = event.graph();
            LOG.debug("Graph {} clear schema cache on meta event", graphName);
            clearSchemaCache(graphName);
        }
    }

    /**
     * Manually reset the JVM-global meta listener flag after detecting that
     * the MetaManager transport reconnected and dropped the underlying gRPC
     * watch. This method is not wired to a MetaManager/MetaDriver reconnect
     * callback today; callers must invoke it explicitly after detecting that
     * condition. Without such a manual reset {@link #metaEventListenerRegistered}
     * would stay {@code true} forever and this JVM would stop receiving
     * cross-node schema cache clear events with no error or warning.
     *
     * <p>TODO: wire this into MetaManager once it exposes a transport
     * reconnect callback (e.g. {@code listenReconnect} /
     * {@code onTransportReconnect}). Until then it must be invoked
     * explicitly by code that detects the reconnect.
     */
    public static void resetMetaListenerForReconnect() {
        if (metaEventListenerRegistered.compareAndSet(true, false)) {
            LOG.warn("Schema cache clear meta listener lost on reconnect - " +
                     "will re-register on next schema operation.");
        }
    }

    public void clearCache(boolean notify) {
        // Same TOCTOU ordering as clearSchemaCache(String): clear nameCache
        // first, then the array attachment, then idCache last.
        this.nameCache.clear();
        this.arrayCaches.clear();
        this.idCache.clear();

        if (notify) {
            this.maybeNotifySchemaCacheClear();
        }
    }

    private void resetCachedAllIfReachedCapacity() {
        if (this.idCache.size() >= this.idCache.capacity()) {
            LOG.warn("Schema cache reached capacity({}): {}",
                     this.idCache.capacity(), this.idCache.size());
            this.cachedTypes().clear();
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.graphParams().loadGraphStore().provider()
            .unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub schemaEventHub = this.graphParams().schemaEventHub();
        schemaEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    private CachedTypes cachedTypes() {
        return this.arrayCaches.cachedTypes();
    }

    private void resetCachedAll(HugeType type) {
        // Set the cache all flag of the schema type to false
        this.cachedTypes().put(type, false);
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
        // Status transitions are internal bookkeeping; notifying here causes a
        // broadcast storm for every updateSchemaStatus() call from background jobs.
    }

    @Override
    protected void addSchema(SchemaElement schema) {
        super.addSchema(schema);

        this.updateCache(schema);

        // Schema additions must always propagate to remote nodes regardless
        // of TASK_SYNC_DELETION (which only gates removal flows).
        this.notifySchemaCacheClear();
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

    @Override
    public void removeSchema(SchemaElement schema) {
        super.removeSchema(schema);

        this.invalidateCache(schema.type(), schema.id());

        this.maybeNotifySchemaCacheClear();
    }

    private void maybeNotifySchemaCacheClear() {
        // Only suppress notifications for removal tasks when
        // TASK_SYNC_DELETION=true: the caller propagates cache invalidation
        // synchronously, so the meta-event broadcast would be redundant.
        if (!this.graph().option(CoreOptions.TASK_SYNC_DELETION)) {
            this.notifySchemaCacheClear();
        }
    }

    private void notifySchemaCacheClear() {
        MetaManager.instance()
                   .notifySchemaCacheClear(this.graph().graphSpace(),
                                           this.graph().name(),
                                           SCHEMA_CACHE_CLEAR_SOURCE);
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
                this.resetCachedAllIfReachedCapacity();

                this.idCache.update(prefixedId, value);

                SchemaElement schema = (SchemaElement) value;
                Id prefixedName = generateId(schema.type(), schema.name());
                this.nameCache.update(prefixedName, schema);
            }
        }

        // update optimized array cache
        this.arrayCaches.updateIfNeeded((SchemaElement) value);

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
                // Note: reload all schema if the cache is inconsistent with storage layer
                this.clearCache(false);
                this.loadAllSchema();
            }
        }
        return (T) value;
    }

    @Override
    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        Boolean cachedAll = this.cachedTypes().getOrDefault(type, false);
        List<T> results;
        if (cachedAll) {
            results = new ArrayList<>();
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
            results = super.getAllSchema(type);
            long free = this.idCache.capacity() - this.idCache.size();
            if (results.size() <= free) {
                // Update cache
                for (T schema : results) {
                    Id prefixedId = generateId(schema.type(), schema.id());
                    this.idCache.update(prefixedId, schema);

                    Id prefixedName = generateId(schema.type(), schema.name());
                    this.nameCache.update(prefixedName, schema);
                }
                this.cachedTypes().putIfAbsent(type, true);
            }
            return results;
        }
    }

    private void loadAllSchema() {
        getAllSchema(HugeType.PROPERTY_KEY);
        getAllSchema(HugeType.VERTEX_LABEL);
        getAllSchema(HugeType.EDGE_LABEL);
        getAllSchema(HugeType.INDEX_LABEL);
    }

    @Override
    public void clear() {
        // Clear schema info firstly
        super.clear();
        this.clearCache(false);
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

        @PerfUtil.Watched
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

        public void remove(HugeType type, Id id) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                return;
            }
            int key = (int) longId;
            V value = null;
            if (key >= this.size) {
                return;
            }
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
    }

    private static class CachedTypes
            extends ConcurrentHashMap<HugeType, Boolean> {

        private static final long serialVersionUID = -2215549791679355996L;
    }
}
