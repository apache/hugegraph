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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.backend.store.ram.IntObjectMap;
import org.apache.hugegraph.backend.tx.SchemaTransactionV2;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.MetaManager.SchemaCacheClearEvent;
import org.apache.hugegraph.perf.PerfUtil;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;

import com.google.common.collect.ImmutableSet;

public class CachedSchemaTransactionV2 extends SchemaTransactionV2 {

    /*
     * V1 and V2 transactions can coexist in the same JVM for graphs with the
     * same name but different backends. Keep their cache entries isolated:
     * besides holding different schema data, their attachments use different
     * SchemaCaches classes and therefore can't be shared safely.
     */
    private static final String ID_CACHE_PREFIX = "schema-v2-id";
    private static final String NAME_CACHE_PREFIX = "schema-v2-name";

    // MetaDriver doesn't expose unlisten, register the meta listener once.
    // Lifecycle: this JVM-global flag is intentionally never reset by
    // unlistenChanges() (the underlying gRPC watch is process-wide). The driver
    // watch self-heals across transport reconnects (PdMetaDriver via KvClient,
    // EtcdMetaDriver via Watch.Listener re-subscribe), so the subscription stays
    // live and the flag staying true is correct.
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

    private static final ConcurrentMap<String, CacheListenerHolder>
            SCHEMA_CACHE_EVENT_LISTENERS = new ConcurrentHashMap<>();

    /**
     * How a clear event is sent to the other Servers, given the graph space
     * and the graph. It throws if the event could not be sent. It is a field,
     * not a direct call, so tests can observe a publish and make one fail
     * without a PD.
     */
    private static volatile BiConsumer<String, String> clearEventPublisher =
            (graphSpace, graph) -> MetaManager.instance().notifySchemaCacheClear(
                    graphSpace, graph, SCHEMA_CACHE_CLEAR_SOURCE);

    private static final PendingClearEvents PENDING_CLEAR_EVENTS =
            new PendingClearEvents();

    private CacheListenerHolder holder;
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

    @Override
    public void close() {
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
        Cache<Id, Object> nameCache = caches.get(cacheName(NAME_CACHE_PREFIX,
                                                           spaceGraphName));
        Cache<Id, Object> idCache = caches.get(cacheName(ID_CACHE_PREFIX,
                                                         spaceGraphName));
        SchemaCaches<?> arrayCaches = idCache == null ?
                                      null : idCache.attachment();
        clearCaches(nameCache, arrayCaches, idCache);
    }

    /**
     * Clear every V2 schema cache of this JVM. Used when the clear-event watch
     * becomes live: whatever was published while it was down never arrived.
     */
    private static void clearAllSchemaCaches() {
        String prefix = ID_CACHE_PREFIX + "-";
        List<String> names = new ArrayList<>(CacheManager.instance()
                                                         .caches().keySet());
        for (String name : names) {
            if (name.startsWith(prefix)) {
                clearSchemaCache(name.substring(prefix.length()));
            }
        }
    }

    /**
     * Drop everything cached for one graph and advance its generation, so a
     * read or a write-through that began before this point can't put its
     * value into the cache afterwards.
     */
    private static void clearCaches(Cache<Id, Object> nameCache,
                                    SchemaCaches<?> arrayCaches,
                                    Cache<Id, Object> idCache) {
        if (arrayCaches == null) {
            /*
             * No transaction has attached the guard yet, so nothing reads or
             * refills these caches.
             */
            if (nameCache != null) {
                nameCache.clear();
            }
            if (idCache != null) {
                idCache.clear();
            }
            return;
        }
        Lock lock = arrayCaches.writeLock();
        lock.lock();
        try {
            /*
             * Clear name cache first so the (name -> id -> object) lookup
             * path of a reader that takes no lock fails fast instead of
             * returning a stale object backed by an already-empty id cache.
             */
            if (nameCache != null) {
                nameCache.clear();
            }
            arrayCaches.clear();
            if (idCache != null) {
                idCache.clear();
            }
        } finally {
            // Last, see SchemaCaches
            arrayCaches.advanceGeneration();
            lock.unlock();
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
                this.invalidateCache(type, id, null, true);
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                this.clearCache(false);
                return true;
            }
            return false;
        };
        EventHub schemaEventHub = this.graphParams().schemaEventHub();
        listenSchemaCacheClear();
        boolean[] opened = {false};
        CacheListenerHolder acquired = SCHEMA_CACHE_EVENT_LISTENERS.compute(
                this.graphParams().spaceGraphName(), (key, existing) -> {
                    if (existing == null || existing.hub != schemaEventHub) {
                        opened[0] = true;
                        if (existing != null) {
                            existing.close();
                        }
                        schemaEventHub.listen(Events.CACHE, listener);
                        this.graphParams().loadGraphStore().provider().listen(this.storeEventListener);
                        return new CacheListenerHolder(listener, schemaEventHub, () -> {
                            try {
                                this.graphParams().loadGraphStore().provider().unlisten(this.storeEventListener);
                            } finally {
                                this.clearCache(false);
                            }
                        });
                    }
                    existing.refCount++;
                    return existing;
                });
        this.holder = acquired;
        this.cacheEventListener = acquired.listener;
        if (opened[0]) {
            /*
             * This JVM just opened the graph. A previous run of this Server
             * may have stopped between a schema write and its clear event,
             * and the set of failed publishes it kept in memory is gone, so
             * tell the other Servers once to drop what they hold.
             */
            this.publishClearEvent();
        }
    }

    private static void listenSchemaCacheClear() {
        synchronized (META_LISTENER_LOCK) {
            if (metaEventListenerRegistered.get()) {
                return;
            }
            try {
                MetaManager.instance().listenSchemaCacheClear(
                        CachedSchemaTransactionV2::handleSchemaCacheClearEvent,
                        CachedSchemaTransactionV2::handleSchemaCacheWatchStarted);
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
     * Runs each time the clear-event watch becomes live: when it first starts
     * and whenever it starts again after a disconnect or a PD leader change.
     * Events published while it was down were not delivered and are not
     * replayed, so every cache of this JVM is dropped, and clear events of
     * this Server that failed to publish are sent again.
     */
    static void handleSchemaCacheWatchStarted() {
        LOG.info("Schema cache clear watch is live, clear all V2 schema caches");
        clearAllSchemaCaches();
        PENDING_CLEAR_EVENTS.retryNow();
    }

    public void clearCache(boolean notify) {
        clearCaches(this.nameCache, this.arrayCaches, this.idCache);

        if (notify) {
            this.publishClearEvent();
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
        CacheListenerHolder ours = this.holder;
        if (ours != null) {
            SCHEMA_CACHE_EVENT_LISTENERS.compute(this.graphParams().spaceGraphName(), (key, existing) -> {
                if (existing == ours) {
                    existing.refCount--;
                }
                return existing;
            });
            this.holder = null;
            this.cacheEventListener = null;
        }
    }

    public static void closeGraph(HugeGraphParams params) {
        SCHEMA_CACHE_EVENT_LISTENERS.computeIfPresent(params.spaceGraphName(), (key, existing) -> {
            if (existing.hub != params.schemaEventHub()) {
                return existing;
            }
            existing.close();
            return null;
        });
    }

    private CachedTypes cachedTypes() {
        return this.arrayCaches.cachedTypes();
    }

    private void resetCachedAll(HugeType type) {
        // Set the cache all flag of the schema type to false
        this.cachedTypes().put(type, false);
    }

    /**
     * Remove one element from the caches and advance the generation, so a
     * read that fetched the element before this point can't cache it again.
     */
    private void invalidateCache(HugeType type, Id id, String name,
                                 boolean resetCachedAll) {
        Lock lock = this.arrayCaches.writeLock();
        lock.lock();
        try {
            this.removeFromCaches(type, id, name);
            if (resetCachedAll) {
                this.resetCachedAll(type);
            }
        } finally {
            // Last, see SchemaCaches
            this.arrayCaches.advanceGeneration();
            lock.unlock();
        }
    }

    // The caller holds the write lock
    private void removeFromCaches(HugeType type, Id id, String name) {
        // remove from id cache and name cache
        Id prefixedId = generateId(type, id);
        Object value = this.idCache.get(prefixedId);
        if (value != null) {
            this.idCache.invalidate(prefixedId);

            SchemaElement schema = (SchemaElement) value;
            Id prefixedName = generateId(schema.type(), schema.name());
            this.nameCache.invalidate(prefixedName);
        }
        if (name != null) {
            this.nameCache.invalidate(generateId(type, name));
        }

        // remove from optimized array cache
        this.arrayCaches.remove(type, id);
    }

    // The caller holds the write lock
    private void putIntoCaches(SchemaElement schema) {
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

    /**
     * Cache an element read from the store, unless the caches were cleared or
     * the element was written since {@code generation} was read. The value
     * may then be older than what the store holds now, and nothing would
     * correct it later because schema caches don't expire.
     */
    private void cacheIfCurrent(SchemaElement schema, long generation) {
        Lock lock = this.arrayCaches.writeLock();
        lock.lock();
        try {
            if (this.arrayCaches.generation() == generation) {
                this.putIntoCaches(schema);
            }
        } finally {
            lock.unlock();
        }
    }

    // Copy an id-cache hit into the array cache, under the same condition
    private void promoteIfCurrent(SchemaElement schema, long generation) {
        Lock lock = this.arrayCaches.writeLock();
        lock.lock();
        try {
            if (this.arrayCaches.generation() == generation) {
                this.arrayCaches.updateIfNeeded(schema);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cache an element this transaction just wrote to the store.
     *
     * @param generation read before the write was sent
     */
    private void cacheWritten(SchemaElement schema, long generation) {
        Lock lock = this.arrayCaches.writeLock();
        lock.lock();
        try {
            if (this.arrayCaches.generation() == generation) {
                this.putIntoCaches(schema);
            } else {
                /*
                 * The caches were cleared while the write was on its way, so
                 * another Server may have replaced the element after this
                 * write. Cache nothing and let the next read ask the store.
                 */
                this.removeFromCaches(schema.type(), schema.id(), schema.name());
                this.resetCachedAll(schema.type());
            }
        } finally {
            /*
             * The write replaced the stored element, so whatever a concurrent
             * read fetched before now is old: don't let it be cached. Last,
             * see SchemaCaches.
             */
            this.arrayCaches.advanceGeneration();
            lock.unlock();
        }
    }

    @Override
    protected void updateSchema(SchemaElement schema,
                                Consumer<SchemaElement> updateCallback) {
        long generation = this.arrayCaches.generation();
        try {
            super.updateSchema(schema, updateCallback);
        } catch (Throwable e) {
            this.writeFailed(schema);
            throw e;
        }

        /*
         * An update must reach the other Servers like an add does: they keep
         * the whole element until told otherwise, and write it back whole on
         * their next update of it. That includes status changes made by
         * background jobs.
         */
        try {
            this.cacheWritten(schema, generation);
        } finally {
            this.publishClearEvent();
        }
    }

    @Override
    protected void addSchema(SchemaElement schema) {
        long generation = this.arrayCaches.generation();
        try {
            super.addSchema(schema);
        } catch (Throwable e) {
            this.writeFailed(schema);
            throw e;
        }

        try {
            this.cacheWritten(schema, generation);
        } finally {
            this.publishClearEvent();
        }
    }

    @Override
    public void removeSchema(SchemaElement schema) {
        try {
            super.removeSchema(schema);
        } catch (Throwable e) {
            this.writeFailed(schema);
            throw e;
        }

        /*
         * Publish whatever task.sync_deletion says: that option only makes
         * the caller wait for the local task, it never reached another JVM.
         */
        try {
            this.invalidateCache(schema.type(), schema.id(), schema.name(),
                                 false);
        } finally {
            this.publishClearEvent();
        }
    }

    /**
     * A schema write is several puts or deletes in the store, so one that
     * threw may have changed the store in part or in full. Treat it like a
     * write: drop the element here, so nothing serves a copy the store no
     * longer matches, and tell the other Servers to do the same.
     */
    private void writeFailed(SchemaElement schema) {
        try {
            this.invalidateCache(schema.type(), schema.id(), schema.name(),
                                 true);
        } finally {
            this.publishClearEvent();
        }
    }

    /**
     * Tell the other Servers to drop their schema caches of this graph. It is
     * called after the schema write is committed, so it never fails the
     * caller: a failed publish is kept and sent again, see
     * {@link PendingClearEvents}.
     */
    private void publishClearEvent() {
        PENDING_CLEAR_EVENTS.publish(this.graph().graphSpace(),
                                     this.graph().name());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        // Read it before any cache or store access, see SchemaCaches
        long generation = this.arrayCaches.generation();

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
                this.cacheIfCurrent((SchemaElement) value, generation);
            }
        } else if (this.arrayCaches.accepts(id)) {
            // update optimized array cache
            this.promoteIfCurrent((SchemaElement) value, generation);
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
                // Note: reload all schema if the cache is inconsistent with storage layer
                this.reloadAllSchema(prefixedName);
            }
        }
        return (T) value;
    }

    /**
     * Clear and load the whole schema after a name was found in the store but
     * not in the cache. One reload runs at a time per graph: a lookup that
     * waited finds its name cached by the reload it waited for. Without that,
     * every concurrent miss would clear the cache again, which also advances
     * the generation and throws away what the other reloads are loading.
     */
    private void reloadAllSchema(Id missedName) {
        synchronized (this.arrayCaches.reloadLock()) {
            if (this.nameCache.get(missedName) != null) {
                return;
            }
            this.clearCache(false);
            this.loadAllSchema();
        }
    }

    @Override
    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        long generation;
        /*
         * A clear can't run while the list is read from the cache, so the
         * result is the complete list or the store is asked, never a part.
         */
        Lock readLock = this.arrayCaches.readLock();
        readLock.lock();
        try {
            generation = this.arrayCaches.generation();
            if (this.cachedTypes().getOrDefault(type, false)) {
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
            }
        } finally {
            readLock.unlock();
        }

        List<T> results = super.getAllSchema(type);
        Lock writeLock = this.arrayCaches.writeLock();
        writeLock.lock();
        try {
            if (this.arrayCaches.generation() != generation) {
                // Cleared or written meanwhile: the list may be old already
                return results;
            }
            long free = this.idCache.capacity() - this.idCache.size();
            if (results.size() <= free) {
                // Update cache
                for (T schema : results) {
                    Id prefixedId = generateId(schema.type(), schema.id());
                    this.idCache.update(prefixedId, schema);

                    Id prefixedName = generateId(schema.type(), schema.name());
                    this.nameCache.update(prefixedName, schema);
                }
                /*
                 * The generation is unchanged, so this is the whole list:
                 * also replace a "not all cached" left by an earlier write
                 */
                this.cachedTypes().put(type, true);
            }
        } finally {
            writeLock.unlock();
        }
        return results;
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
        try {
            super.clear();
        } finally {
            // A clear that threw may have removed part of the schema
            try {
                this.clearCache(false);
            } finally {
                this.publishClearEvent();
            }
        }
    }

    /**
     * Clear events of this Server that could not be published.
     * <p>
     * A clear event is sent after its schema write is committed, so a failed
     * publish must not fail the write. The graph is kept here instead and the
     * event is sent again: after a delay that doubles up to a limit, and at
     * once when the clear-event watch becomes live. One event says "drop your
     * caches of this graph", so one successful publish covers every earlier
     * failure for that graph.
     * <p>
     * The set lives in memory. If the Server stops before a retry succeeds,
     * the publish it does on the next graph open covers it. If the Server
     * never returns, the other Servers keep what they cached until the next
     * event for that graph or their next watch restart.
     */
    static final class PendingClearEvents {

        private static final long RETRY_MIN_DELAY = 1000L;
        private static final long RETRY_MAX_DELAY = 30000L;

        private final ConcurrentMap<GraphRef, Long> pending =
                new ConcurrentHashMap<>();
        private final AtomicLong failures = new AtomicLong();

        // Guarded by this
        private ScheduledExecutorService executor;
        private ScheduledFuture<?> retry;
        private long retryDelay = RETRY_MIN_DELAY;
        // The number of the newest scheduled retry
        private long retryEpoch;

        void publish(String graphSpace, String graph) {
            try {
                clearEventPublisher.accept(graphSpace, graph);
            } catch (RuntimeException e) {
                LOG.warn("Failed to publish the schema cache clear event of " +
                         "graph '{}/{}', it will be sent again",
                         graphSpace, graph, e);
                /*
                 * The number tells a retry whether the graph failed again
                 * after the publish the retry is about to count as done.
                 */
                this.pending.put(new GraphRef(graphSpace, graph),
                                 this.failures.incrementAndGet());
                this.scheduleRetry(false);
            }
        }

        void retryNow() {
            this.scheduleRetry(true);
        }

        int size() {
            return this.pending.size();
        }

        private synchronized void scheduleRetry(boolean now) {
            if (this.pending.isEmpty()) {
                return;
            }
            if (this.retry != null && !this.retry.isDone()) {
                if (!now) {
                    return;
                }
                this.retry.cancel(false);
            }
            if (this.executor == null) {
                this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread thread = new Thread(r, "schema-cache-clear-retry");
                    thread.setDaemon(true);
                    return thread;
                });
            }
            long delay = now ? 0L : this.retryDelay;
            long epoch = ++this.retryEpoch;
            this.retry = this.executor.schedule(() -> this.retryPending(epoch),
                                                delay, TimeUnit.MILLISECONDS);
        }

        private void retryPending(long epoch) {
            List<Map.Entry<GraphRef, Long>> entries =
                    new ArrayList<>(this.pending.entrySet());
            for (Map.Entry<GraphRef, Long> entry : entries) {
                GraphRef graph = entry.getKey();
                try {
                    clearEventPublisher.accept(graph.graphSpace(),
                                               graph.graph());
                    // Keep it if the graph failed again in the meantime
                    this.pending.remove(graph, entry.getValue());
                } catch (RuntimeException e) {
                    LOG.warn("Failed again to publish the schema cache " +
                             "clear event of graph '{}/{}': {}",
                             graph.graphSpace(), graph.graph(), e.toString());
                }
            }
            synchronized (this) {
                if (epoch != this.retryEpoch) {
                    /*
                     * A watch start scheduled a newer retry while this one
                     * was running: that one goes on, or there would be two.
                     */
                    return;
                }
                this.retry = null;
                if (this.pending.isEmpty()) {
                    this.retryDelay = RETRY_MIN_DELAY;
                    return;
                }
                this.retryDelay = Math.min(this.retryDelay * 2L,
                                           RETRY_MAX_DELAY);
                this.scheduleRetry(false);
            }
        }
    }

    private record GraphRef(String graphSpace, String graph) {
    }

    /**
     * The per-graph state shared by every transaction of this JVM: the array
     * caches, and the guard that keeps an old value out of all the caches.
     * <p>
     * Schema caches never expire, so a value cached after it was replaced in
     * the store stays wrong until the next clear. Every clear, removal and
     * local write therefore advances {@link #generation} under the write
     * lock. A reader notes the generation before it looks into a cache or
     * asks the store, and caches what it got only if the generation is still
     * the same, checked under the same lock. The store is never called with
     * that lock held.
     * <p>
     * The generation is advanced as the last step of a change, after the
     * caches were updated. Readers note it without a lock, so one that sees
     * the new generation must also see the caches as the change left them.
     * Advancing first would let a reader note the new generation, take the
     * old value from the id cache and copy it to the array cache afterwards.
     */
    private static final class SchemaCaches<V extends SchemaElement> {

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final Object reloadLock = new Object();
        private volatile long generation;

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

        long generation() {
            return this.generation;
        }

        // The caller holds the write lock
        void advanceGeneration() {
            this.generation++;
        }

        Lock readLock() {
            return this.lock.readLock();
        }

        Lock writeLock() {
            return this.lock.writeLock();
        }

        Object reloadLock() {
            return this.reloadLock;
        }

        // Whether the array cache has a slot for this id
        boolean accepts(Id id) {
            if (!id.number()) {
                return false;
            }
            long longId = id.asLong();
            return longId > 0L && longId < this.size;
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
            if (longId >= this.size) {
                return null;
            }
            int key = (int) longId;
            return switch (type) {
                case PROPERTY_KEY -> this.pks.get(key);
                case VERTEX_LABEL -> this.vls.get(key);
                case EDGE_LABEL -> this.els.get(key);
                case INDEX_LABEL -> this.ils.get(key);
                default -> null;
            };
        }

        public void set(HugeType type, Id id, V value) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                assert false : id;
                return;
            }
            if (longId >= this.size) {
                return;
            }
            int key = (int) longId;
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
            if (longId >= this.size) {
                return;
            }
            int key = (int) longId;
            V value = null;
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
