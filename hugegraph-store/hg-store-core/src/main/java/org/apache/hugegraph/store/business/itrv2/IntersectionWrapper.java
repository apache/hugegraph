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

import java.util.List;
import java.util.function.ToLongFunction;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class IntersectionWrapper<T> {

    private Roaring64Bitmap workBitmap;
    private final Roaring64Bitmap resultBitmap;
    private final ScanIterator iterator;
    private final ToLongFunction<T> hashFunction;
    private boolean matchAll;

    public IntersectionWrapper(ScanIterator iterator, ToLongFunction<T> hashFunction) {
        this.iterator = iterator;
        this.hashFunction = hashFunction;
        this.workBitmap = new Roaring64Bitmap();
        this.resultBitmap = new Roaring64Bitmap();
        this.matchAll = false;
    }

    /**
     * Record elements with identical hash values in the iterator
     *
     * @param iterator         iterator
     * @param hashFunction     mapping the element to a long value
     * @param matchAllIterator a value that all exists in the iterator( MultiListIterator)
     */
    public IntersectionWrapper(ScanIterator iterator, ToLongFunction<T> hashFunction,
                               boolean matchAllIterator) {
        this(iterator, hashFunction);
        this.matchAll = matchAllIterator;
    }

    public void proc() {
        if (matchAll && iterator instanceof MultiListIterator) {
            var mIterators = ((MultiListIterator) iterator).getIterators();
            if (mIterators.size() > 1) {
                procMulti(mIterators);
            }
            return;
        }

        procSingle(this.iterator, false);
    }

    /**
     * Compute the intersection of all iterators in a multi-list iterator
     *
     * @param iterators iterators
     */
    private void procMulti(List<ScanIterator> iterators) {
        var itr = iterators.get(0);
        procSingle(itr, true);

        for (int i = 1; i < iterators.size(); i++) {
            // change last round result to the work map
            workBitmap = resultBitmap.clone();
            resultBitmap.clear();
            procSingle(iterators.get(i), false);
        }
    }

    private void procSingle(ScanIterator itr, boolean firstRound) {
        while (itr.hasNext()) {
            var n = itr.next();
            if (n == null) {
                continue;
            }
            var key = hashFunction.applyAsLong((T) n);

            if (firstRound) {
                resultBitmap.add(key);
            } else {
                if (workBitmap.contains(key)) {
                    resultBitmap.add(key);
                } else {
                    workBitmap.add(key);
                }
            }
        }
        workBitmap.clear();
    }

    /**
     * return contains
     *
     * @param o input element
     * @return true: may exist; false: definitely does not exist
     */
    public boolean contains(T o) {
        return resultBitmap.contains(hashFunction.applyAsLong(o));
    }
}

