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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

public class MapUnionIterator<T, E> implements ScanIterator {

    private final List<ScanIterator> iteratorList;

    private final Function<T, E> keyFunction;

    private final Map<E, T> map = new HashMap<>();

    private Iterator<T> iterator;

    private boolean flag = false;

    public MapUnionIterator(List<ScanIterator> iteratorList, Function<T, E> keyFunction) {
        this.iteratorList = iteratorList;
        this.keyFunction = keyFunction;
    }

    @Override
    public boolean hasNext() {
        if (!this.flag) {
            this.proc();
        }
        return this.iterator.hasNext();
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public <T> T next() {
        return (T) this.iterator.next();
    }

    @Override
    public void close() {
        iteratorList.forEach(ScanIterator::close);
        this.map.clear();
    }

    private void proc() {
        for (ScanIterator itr : this.iteratorList) {
            while (itr.hasNext()) {
                var item = (T) itr.next();
                if (item != null) {
                    map.put(keyFunction.apply(item), item);
                }
            }
        }

        this.iterator = map.values().iterator();
        this.flag = true;
    }
}
