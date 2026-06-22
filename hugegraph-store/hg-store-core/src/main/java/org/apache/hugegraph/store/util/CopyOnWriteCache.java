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

package org.apache.hugegraph.store.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CopyOnWriteCache<K, V> implements ConcurrentMap<K, V> {

    // Scheduled executor service for periodically clearing the cache.
    private final ScheduledExecutorService scheduledExecutor;

    // The underlying map used to store key-value pairs in this cache.
    private volatile Map<K, V> map;

    /**
     * Constructs a new CopyOnWriteCache with a specified effective time.
     *
     * @param effectiveTime The time interval in milliseconds after which the cache will be cleared.
     */
    public CopyOnWriteCache(long effectiveTime) {
        // Initialize the map as an empty map at the beginning.
        this.map = Collections.emptyMap();
        // Create a single-threaded scheduled executor to manage cache clearing.
        scheduledExecutor = Executors.newScheduledThreadPool(1);
        // Schedule the clear task to run at fixed intervals defined by effectiveTime.
        scheduledExecutor.scheduleWithFixedDelay(this::clear, effectiveTime, effectiveTime,
                                                 TimeUnit.MILLISECONDS);
    }

    /**
     * Checks if the specified key is present in the cache.
     *
     * @param k The key to check for existence.
     * @return true if the key is found in the cache; otherwise, false.
     */
    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    /**
     * Checks if the specified value is present in the cache.
     *
     * @param v The value to check for existence.
     * @return true if the value is found in the cache; otherwise, false.
     */
    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    /**
     * Returns a set view of the mappings contained in this cache.
     *
     * @return a set of entries representing the key-value pairs in the cache.
     */
    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param k The key whose associated value is to be returned.
     * @return the value associated with the key, or null if the key is not found.
     */
    @Override
    public V get(Object k) {
        return map.get(k);
    }

    /**
     * Checks if the cache is empty.
     *
     * @return true if the cache contains no key-value mappings; otherwise, false.
     */
    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Returns a set view of the keys contained in this cache.
     *
     * @return a set of keys present in the cache.
     */
    @NotNull
    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    /**
     * Returns the number of key-value pairs in the cache.
     *
     * @return the size of the cache.
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * Returns a collection view of the values contained in this cache.
     *
     * @return a collection of values present in the cache.
     */
    @NotNull
    @Override
    public Collection<V> values() {
        return map.values();
    }

    /**
     * Clears all entries from the cache, resetting it to an empty state.
     */
    @Override
    public synchronized void clear() {
        this.map = Collections.emptyMap();
    }

    /**
     * Associates the specified value with the specified key in the cache.
     * If the key already exists, the previous value is replaced.
     *
     * @param k The key with which the specified value is to be associated.
     * @param v The value to be associated with the specified key.
     * @return the previous value associated with the key, or null if there was no mapping for
     * the key.
     */
    @Override
    public synchronized V put(K k, V v) {
        Map<K, V> copy = new HashMap<>(this.map);
        V prev = copy.put(k, v);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    /**
     * Copies all of the mappings from the specified map to this cache.
     *
     * @param entries The map whose mappings are to be copied.
     */
    @Override
    public synchronized void putAll(@NotNull Map<? extends K, ? extends V> entries) {
        Map<K, V> copy = new HashMap<>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    /**
     * Removes the mapping for the specified key from this cache if present.
     *
     * @param key The key whose mapping is to be removed from the cache.
     * @return the previous value associated with the key, or null if there was no mapping for
     * the key.
     */
    @Override
    public synchronized V remove(Object key) {
        Map<K, V> copy = new HashMap<>(this.map);
        V prev = copy.remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    /**
     * If the specified key is not already associated with a value, associates it with the given
     * value.
     * Otherwise, returns the current value associated with the key.
     *
     * @param k The key with which the specified value is to be associated.
     * @param v The value to be associated with the specified key.
     * @return the previous value associated with the key, or null if there was no mapping for
     * the key.
     */
    @Override
    public synchronized V putIfAbsent(K k, V v) {
        if (!containsKey(k)) {
            return put(k, v);
        } else {
            return get(k);
        }
    }

    /**
     * Removes the entry for the specified key only if it is currently mapped to the specified
     * value.
     *
     * @param k The key whose mapping is to be removed.
     * @param v The value expected to be associated with the key.
     * @return true if the mapping was removed; otherwise, false.
     */
    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces the entry for the specified key only if it is currently mapped to the specified
     * original value.
     *
     * @param k           The key whose mapping is to be replaced.
     * @param original    The expected value to be associated with the key.
     * @param replacement The value to be associated with the key if the original value is present.
     * @return true if the mapping was replaced; otherwise, false.
     */
    @Override
    public synchronized boolean replace(@NotNull K k, @NotNull V original, @NotNull V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces the entry for the specified key with the given value.
     *
     * @param k The key whose mapping is to be replaced.
     * @param v The new value to be associated with the key.
     * @return the previous value associated with the key, or null if there was no mapping for
     * the key.
     */
    @Override
    public synchronized V replace(K k, V v) {
        if (containsKey(k)) {
            return put(k, v);
        } else {
            return null;
        }
    }

    public void close(){
        scheduledExecutor.shutdown();
        try {
            boolean isTerminated = scheduledExecutor.awaitTermination(30, TimeUnit.SECONDS);
            if (!isTerminated) {
                List<Runnable> runnables = scheduledExecutor.shutdownNow();
                log.info("CopyOnWriteCache shutting down with {} tasks left", runnables.size());

                boolean isNowTerminated = scheduledExecutor.awaitTermination(30, TimeUnit.SECONDS);
                if (!isNowTerminated) {
                    log.warn("Failed to shutdown CopyOnWriteCache thread pool");
                }
            }
        }catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
