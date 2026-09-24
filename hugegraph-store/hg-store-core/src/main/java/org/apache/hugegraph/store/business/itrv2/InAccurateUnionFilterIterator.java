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

import java.util.NoSuchElementException;
import java.util.function.ToLongFunction;

import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * Inaccurate Filter, using bit map
 *
 * @param <T>
 */
public class InAccurateUnionFilterIterator<T> implements ScanIterator {

    private final Roaring64Bitmap workBitmap;

    private final ToLongFunction<T> toLongFunction;

    private final ScanIterator iterator;

    private T current;

    public InAccurateUnionFilterIterator(ScanIterator iterator, ToLongFunction<T> toLongFunction) {
        this.iterator = iterator;
        this.workBitmap = new Roaring64Bitmap();
        if (toLongFunction == null) {
            throw new NullPointerException("toLongFunction cannot be null");
        }
        this.toLongFunction = toLongFunction;
    }

    @Override
    public boolean hasNext() {
        current = null;
        while (iterator.hasNext()) {
            var element = (T) iterator.next();
            if (element == null) {
                continue;
            }

            var key = toLongFunction.applyAsLong(element);
            if (!workBitmap.contains(key)) {
                current = element;
                workBitmap.add(key);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    @Override
    public <E> E next() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return (E) current;
    }

    @Override
    public long count() {
        return iterator.count();
    }

    @Override
    public byte[] position() {
        return iterator.position();
    }

    @Override
    public void seek(byte[] position) {
        iterator.seek(position);
    }

    @Override
    public void close() {
        iterator.close();
        this.workBitmap.clear();
    }
}
