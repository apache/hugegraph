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

import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.util.E;

public final class LevelCache extends AbstractCache<Id, Object> {

    // For multi-layer caches
    private final AbstractCache<Id, Object>[] caches;

    @SuppressWarnings("unchecked")
    public LevelCache(AbstractCache<Id, Object> lavel1,
                      AbstractCache<Id, Object> lavel2) {
        super(lavel2.capacity());
        super.expire(lavel2.expire());
        this.caches = new AbstractCache[]{lavel1, lavel2};
    }

    @Override
    public void traverse(Consumer<Object> consumer) {
        this.last().traverse(consumer);
    }

    @Override
    public long size() {
        return this.last().size();
    }

    @Override
    public void clear() {
        for (AbstractCache<Id, Object> cache : this.caches) {
            cache.clear();
        }
    }

    @Override
    public void expire(long ms) {
        super.expire(ms);
        for (AbstractCache<Id, Object> cache : this.caches) {
            cache.expire(ms);
        }
    }

    @Override
    public boolean containsKey(Id id) {
        for (AbstractCache<Id, Object> cache : this.caches) {
            if (cache.containsKey(id)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Object access(Id id) {
        for (AbstractCache<Id, Object> cache : this.caches) {
            // Priority access to the previous level
            Object value = cache.access(id);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    protected boolean write(Id id, Object value, long timeOffset) {
        boolean success = false;
        for (AbstractCache<Id, Object> cache : this.caches) {
            success |= cache.write(id, value, timeOffset);
        }
        return success;
    }

    @Override
    protected void remove(Id id) {
        for (AbstractCache<Id, Object> cache : this.caches) {
            cache.remove(id);
        }
    }

    @Override
    protected Iterator<CacheNode<Id, Object>> nodes() {
        ExtendableIterator<CacheNode<Id, Object>> iters;
        iters = new ExtendableIterator<>();
        for (AbstractCache<Id, Object> cache : this.caches) {
            iters.extend(cache.nodes());
        }
        return iters;
    }

    protected AbstractCache<Id, Object> last() {
        final int length = this.caches.length;
        E.checkState(length > 0,
                     "Expect at least one cache in LevelCache, but got %s",
                     length);
        return this.caches[length - 1];
    }
}
