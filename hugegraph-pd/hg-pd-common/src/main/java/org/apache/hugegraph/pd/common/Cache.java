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

package org.apache.hugegraph.pd.common;

import org.apache.hugegraph.pd.util.DefaultThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// FIXME: issues may arise in concurrent scenarios.
public class Cache<T> implements Closeable {

    ScheduledExecutorService ex =
            Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("hg-cache"));
    private ConcurrentMap<String, CacheValue> map = new ConcurrentHashMap<>();
    private ScheduledFuture<?> future;
    private Runnable checker = () -> {
        for (Map.Entry<String, CacheValue> e : map.entrySet()) {
            if (e.getValue().getValue() == null) {
                map.remove(e.getKey());
            }
        }
    };

    public Cache() {
        future = ex.scheduleWithFixedDelay(checker, 1, 1, TimeUnit.SECONDS);
    }

    public CacheValue put(String key, T value, long ttl) {
        return map.put(key, new CacheValue(value, ttl));
    }

    public T get(String key) {
        CacheValue value = map.get(key);
        if (value == null) {
            return null;
        }
        T t = value.getValue();
        if (t == null) {
            map.remove(key);
        }
        return t;
    }

    public boolean keepAlive(String key, long ttl) {
        CacheValue value = map.get(key);
        if (value == null) {
            return false;
        }
        value.keepAlive(ttl);
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            future.cancel(true);
            ex.shutdownNow();
        } catch (Exception e) {
            try {
                ex.shutdownNow();
            } catch (Exception ex) {

            }
        }
    }

    public class CacheValue {

        private final T value;
        long outTime;

        protected CacheValue(T value, long ttl) {
            this.value = value;
            this.outTime = System.currentTimeMillis() + ttl;
        }

        protected T getValue() {
            if (System.currentTimeMillis() >= outTime) {
                return null;
            }
            return value;
        }

        protected void keepAlive(long ttl) {
            this.outTime = System.currentTimeMillis() + ttl;
        }

    }
}
