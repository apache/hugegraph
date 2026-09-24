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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.MultiPartitionIterator;

/**
 * A group of same-type iterators, output sequentially by iterator
 */
public class MultiListIterator implements ScanIterator {

    /**
     * iterator list
     */
    private final List<ScanIterator> iterators;

    /**
     * iterator of iterator list
     */
    private Iterator<ScanIterator> innerListIterator;

    /**
     * current element
     */
    private ScanIterator innerIterator;

    public MultiListIterator() {
        this.iterators = new CopyOnWriteArrayList<>();
    }

    public MultiListIterator(List<ScanIterator> iterators) {
        this.iterators = new CopyOnWriteArrayList<>(iterators);
    }

    /**
     * Add the iterator to the scanning iterator list
     *
     * @param iterator iterator to add
     */
    public void addIterator(ScanIterator iterator) {
        this.iterators.add(iterator);
    }

    public List<ScanIterator> getIterators() {
        return iterators;
    }

    /**
     * Get inner iterator
     */
    private void getInnerIterator() {
        if (this.innerIterator != null && this.innerIterator.hasNext()) {
            return;
        }

        // close prev one
        if (this.innerIterator != null) {
            this.innerIterator.close();
        }

        if (this.innerListIterator == null) {
            this.innerListIterator = this.iterators.iterator();
        }

        while (this.innerListIterator.hasNext()) {
            this.innerIterator = this.innerListIterator.next();
            if (this.innerIterator.hasNext()) {
                return;
            } else {
                // whole empty
                this.innerIterator.close();
            }
        }

        this.innerIterator = null;
    }

    @Override
    public boolean hasNext() {
        getInnerIterator();
        return this.innerIterator != null;
    }

    @Override
    public boolean isValid() {
        getInnerIterator();
        if (this.innerIterator != null) {
            return this.innerIterator.isValid();
        }
        return true;
    }

    /**
     * Close iterator
     */
    @Override
    public void close() {
        if (this.innerIterator != null) {
            this.innerIterator.close();
        }
        if (this.innerListIterator != null) {
            while (this.innerListIterator.hasNext()) {
                this.innerListIterator.next().close();
            }
        }
        this.iterators.clear();
    }

    @Override
    public <T> T next() {
        return this.innerIterator.next();
    }

    @Override
    public long count() {
        long count = 0;
        while (hasNext()) {
            next();
            count += 1;
        }
        return count;
    }

    @Override
    public byte[] position() {
        return this.innerIterator.position();
    }

    @Override
    public void seek(byte[] position) {
        if (this.iterators.size() == 1) {
            // range scan or prefix scan
            if (this.innerIterator instanceof MultiPartitionIterator) {
                this.innerIterator.seek(position);
            }
        }
    }
}
