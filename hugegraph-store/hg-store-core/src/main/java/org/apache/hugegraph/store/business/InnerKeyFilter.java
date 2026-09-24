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

package org.apache.hugegraph.store.business;

import java.util.Arrays;

import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.term.Bits;

public class InnerKeyFilter<T extends BackendColumn> implements ScanIterator {

    final int codeFrom;
    final int codeTo;
    // Whether to perform code filtering, enable this option, return the key's tail containing code
    final boolean codeFilter;
    ScanIterator iterator;
    T current = null;

    public InnerKeyFilter(ScanIterator iterator) {
        this.iterator = iterator;
        this.codeFrom = Integer.MIN_VALUE;
        this.codeTo = Integer.MAX_VALUE;
        this.codeFilter = false;
        moveNext();
    }

    public InnerKeyFilter(ScanIterator iterator, boolean codeFilter) {
        this.iterator = iterator;
        this.codeFrom = Integer.MIN_VALUE;
        this.codeTo = Integer.MAX_VALUE;
        this.codeFilter = codeFilter;
        moveNext();
    }

    public InnerKeyFilter(ScanIterator iterator, int codeFrom, int codeTo) {
        this.iterator = iterator;
        this.codeFrom = codeFrom;
        this.codeTo = codeTo;
        this.codeFilter = true;
        moveNext();
    }

    private void moveNext() {
        current = null;
        if (codeFilter) {
            while (iterator.hasNext()) {
                T t = iterator.next();
                int code = Bits.getShort(t.name, t.name.length - Short.BYTES);
                if (code >= codeFrom && code < codeTo) {
                    current = t;
                    break;
                }
            }
        } else {
            if (iterator.hasNext()) {
                current = iterator.next();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    @Override
    public T next() {
        T column = current;
        if (!codeFilter)
        // Remove the image ID and hash suffix
        {
            column.name = Arrays.copyOfRange(column.name, Short.BYTES,
                                             column.name.length - Short.BYTES);
        } else // Remove graph ID
        {
            column.name = Arrays.copyOfRange(column.name, Short.BYTES,
                                             column.name.length);
        }
        moveNext();
        return column;
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public long count() {
        return iterator.count();
    }
}
