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

package org.apache.hugegraph.pd.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @param <K>
 * @param <V>
 */
public class HgMapCache<K, V> {

    private Map<K, V> cache = new ConcurrentHashMap<K, V>();
    private Supplier<Boolean> expiry;

    private HgMapCache(Supplier<Boolean> expiredPolicy) {
        this.expiry = expiredPolicy;
    }

    public static HgMapCache expiredOf(long interval) {
        return new HgMapCache(new CycleIntervalPolicy(interval));
    }

    private boolean isExpired() {
        if (expiry != null && expiry.get()) {
            cache.clear();
            return true;
        }
        return false;
    }

    public void put(K key, V value) {
        if (key == null || value == null) {
            return;
        }
        this.cache.put(key, value);
    }

    public V get(K key) {
        if (isExpired()) {
            return null;
        }
        return this.cache.get(key);
    }

    public void removeAll() {
        this.cache.clear();
    }

    public boolean remove(K key) {
        if (key != null) {
            this.cache.remove(key);
            return true;
        }
        return false;
    }

    public Map<K, V> getAll() {
        return this.cache;
    }

    private static class CycleIntervalPolicy implements Supplier<Boolean> {

        private long expireTime = 0;
        private long interval = 0;

        public CycleIntervalPolicy(long interval) {
            this.interval = interval;
            init();
        }

        private void init() {
            expireTime = System.currentTimeMillis() + interval;
        }

        @Override
        public Boolean get() {
            if (System.currentTimeMillis() > expireTime) {
                init();
                return true;
            }
            return false;
        }

    }

}
