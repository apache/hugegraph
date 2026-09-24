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

import org.apache.hugegraph.store.HgKvIterator;

public class StreamLimitIterator<E> implements HgKvIterator<E> {

    private final HgKvIterator<E> iterator;
    private final int limit;

    private int count = 0;

    public StreamLimitIterator(HgKvIterator<E> iterator, Integer limit) {
        this.iterator = iterator;
        this.limit = limit;
    }

    @Override
    public byte[] key() {
        return iterator.key();
    }

    @Override
    public byte[] value() {
        return iterator.value();
    }

    @Override
    public void close() {
        iterator.close();
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
    public boolean hasNext() {
        return count < limit && iterator.hasNext();
    }

    @Override
    public E next() {
        count += 1;
        return iterator.next();
    }
}
