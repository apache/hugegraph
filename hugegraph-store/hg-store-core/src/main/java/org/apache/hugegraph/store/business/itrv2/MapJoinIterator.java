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

public class MapJoinIterator<T, E> implements ScanIterator {

    private final List<ScanIterator> iteratorList;

    private final Function<T, E> keyFunction;

    private final Map<E, T> map = new HashMap<>();

    private Iterator<T> iterator;

    private int loc = -1;

    private boolean flag;

    /**
     * Intersection of multiple iterators
     *
     * @param iteratorList iterator list
     * @param loc          the location of the iterator having smallest size
     * @param keyFunction  key mapping mapping
     */
    public MapJoinIterator(List<ScanIterator> iteratorList, int loc, Function<T, E> keyFunction) {
        assert (iteratorList != null);
        assert (loc >= 0 && loc < iteratorList.size());
        this.iteratorList = iteratorList;
        this.keyFunction = keyFunction;
        this.loc = loc;
        this.flag = false;
    }

    @Override
    public boolean hasNext() {
        if (!flag) {
            proc();
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

    public void reset() {
        this.iterator = this.map.values().iterator();
    }

    private void proc() {
        var itr = iteratorList.get(loc);
        while (itr.hasNext()) {
            var tmp = (T) itr.next();
            if (tmp != null) {
                map.put(keyFunction.apply(tmp), tmp);
            }
        }

        for (int i = 0; i < iteratorList.size(); i++) {

            if (i == loc) {
                continue;
            }

            var workMap = new HashMap<E, T>();

            itr = iteratorList.get(i);
            while (itr.hasNext()) {
                var tmp = (T) itr.next();
                if (tmp != null) {
                    var key = keyFunction.apply(tmp);
                    if (map.containsKey(key)) {
                        workMap.put(key, tmp);
                    }
                }
            }

            map.clear();
            map.putAll(workMap);
        }

        this.iterator = this.map.values().iterator();

        this.flag = true;
    }
}
