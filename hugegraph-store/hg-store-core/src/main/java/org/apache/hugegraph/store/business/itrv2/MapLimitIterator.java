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

package org.apache.hugegraph.store.business.itrv2;

import java.util.Set;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;

/**
 * Deduplicate an iterator with exact deduplication for the first SET_MAX_SIZE elements, then
 * return the remaining elements directly
 *
 * @param <T>
 */
public class MapLimitIterator<T> implements ScanIterator {

    private static final Integer SET_MAX_SIZE = 100000;
    private final ScanIterator iterator;
    private final Set<T> set;
    private T current = null;

    public MapLimitIterator(ScanIterator iterator) {
        this.iterator = iterator;
        set = new ConcurrentHashSet<>();
    }

    /**
     * {@inheritDoc}
     * Returns whether the next element exists. Checks if there is another available element in
     * the collection; returns true if so, otherwise false. If the current element is null or
     * already exists in the set, it will skip this element and continue checking the next one.
     * After checking all eligible elements, calling the hasNext method again will re-check the
     * elements. If conditions are met (i.e., not null and not contained in the set), the current
     * element will be added to the set and return true. When the set already contains
     * SET_MAX_SIZE elements, no new elements will be added, and it will return false
     *
     * @return whether the next element exists
     */
    @Override
    public boolean hasNext() {
        current = null;
        while (iterator.hasNext()) {
            var tmp = (T) iterator.next();
            if (tmp != null && !set.contains(tmp)) {
                current = tmp;
                break;
            }
        }

        // Control the size of the set
        if (current != null && set.size() <= SET_MAX_SIZE) {
            set.add(current);
        }

        return current != null;
    }

    /**
     * {@inheritDoc}
     * return current object
     *
     * @return The current object is a reference of type T1
     */
    @Override
    public <T1> T1 next() {
        return (T1) current;
    }

    /**
     * Whether the iterator is valid
     *
     * @return Whether the iterator is valid
     */
    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    /**
     * Iterator count
     *
     * @return
     */
    @Override
    public long count() {
        return iterator.count();
    }

    /**
     * Current position of iterator
     *
     * @return Current position of iterator
     */
    @Override
    public byte[] position() {
        return iterator.position();
    }

    /**
     * {@inheritDoc}
     * Move the file pointer to the target position
     *
     * @param position target position
     */
    @Override
    public void seek(byte[] position) {
        iterator.seek(position);
    }

    /**
     * close iterator
     */
    @Override
    public void close() {
        iterator.close();
        this.set.clear();
    }
}
