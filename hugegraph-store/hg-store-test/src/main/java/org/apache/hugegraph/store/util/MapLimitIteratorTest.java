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

package org.apache.hugegraph.store.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.MapLimitIterator;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;
import org.junit.Test;

/**
 * An index scan that loads properties returns a new BaseVertex per index entry, and BaseVertex
 * has no equals/hashCode, so LIMIT_DEDUP must dedup on the element id.
 */
public class MapLimitIteratorTest {

    private static final int KEYS = 10;

    @Test
    public void testLimitDedupDropsVerticesWithSameId() {
        ScanIterator filter = new MapLimitIterator<BaseElement>(scanTwice(), BaseElement::id);

        assertEquals(KEYS, drain(filter));
    }

    private static ScanIterator scanTwice() {
        List<BaseElement> rows = new ArrayList<>();
        for (int round = 0; round < 2; round++) {
            for (int key = 0; key < KEYS; key++) {
                rows.add(new BaseVertex(IdGenerator.of(key)));
            }
        }
        Iterator<BaseElement> iterator = rows.iterator();
        return new ScanIterator() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public boolean isValid() {
                return iterator.hasNext();
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> T next() {
                return (T) iterator.next();
            }

            @Override
            public void close() {
            }
        };
    }

    private static int drain(ScanIterator iterator) {
        int count = 0;
        try {
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
        } finally {
            iterator.close();
        }
        return count;
    }
}
