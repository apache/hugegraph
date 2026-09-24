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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;

public class MapValueFilterIterator<K> implements Iterator<K> {

    Iterator<Map.Entry<K, Integer>> mapIterator;
    private final IntPredicate filter;
    private K value;

    public MapValueFilterIterator(Map<K, Integer> map, IntPredicate filter) {
        this.mapIterator = map.entrySet().iterator();
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        while (mapIterator.hasNext()) {
            Map.Entry<K, Integer> entry = mapIterator.next();
            if (filter.test(entry.getValue())) {
                value = entry.getKey();
                return true;
            }
        }
        this.value = null;
        return false;
    }

    @Override
    public K next() {
        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }
}
