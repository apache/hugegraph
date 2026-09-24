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

package org.apache.hugegraph.store.client.query;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.client.HgStoreNodePartitioner;
import org.apache.hugegraph.store.query.QueryTypeParam;
import org.apache.hugegraph.structure.BaseElement;

import java.util.*;

/**
 * Strictly sorted in the order of the ID scan
 * The misalignment issue must be properly handled
 *
 * @param <E>
 */
public class StreamStrictOrderIterator<E> implements HgKvIterator<E> {

    private final Iterator<QueryTypeParam> ids;

    private final Map<String, StreamKvIterator<E>> iteratorMap = new HashMap<>();

    private E data;

    private final String graph;

    private final HgStoreNodePartitioner nodePartitioner;

    public StreamStrictOrderIterator(List<QueryTypeParam> ids, List<HgKvIterator<E>> iteratorList,
                                     String graph, HgStoreNodePartitioner nodePartitioner) {
        this.ids = ids.iterator();
        for (var itr : iteratorList) {
            var itr2 = (StreamKvIterator<E>) itr;
            iteratorMap.put(itr2.getAddress(), itr2);
        }
        this.graph = graph;
        this.nodePartitioner = nodePartitioner;
    }

    @Override
    public byte[] key() {
        return new byte[0];
    }

    @Override
    public byte[] value() {
        return new byte[0];
    }

    @Override
    public void close() {
        for (StreamKvIterator<E> itr : this.iteratorMap.values()) {
            itr.close();
        }
    }

    @Override
    public byte[] position() {
        return new byte[0];
    }

    @Override
    public void seek(byte[] position) {

    }

    @Override
    public boolean hasNext() {
        data = null;

        while (ids.hasNext()) {
            try {
                var param = ids.next();
                var addr = this.nodePartitioner.partition(graph, param.getCode());
                var itr = iteratorMap.get(addr);
                if (itr != null && itr.hasNext()) {
                    var t = (BaseElement) itr.next();
                    if (Arrays.equals(t.id().asBytes(), param.getStart())) {
                        this.data = (E) t;
                        return true;
                    }
                }
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public E next() {
        if (data == null) {
            throw new NoSuchElementException();
        }
        return data;
    }
}
