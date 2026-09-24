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

import java.util.Random;

import org.apache.hugegraph.store.HgKvIterator;

//TODO Only caller use this by if (iter.hasNext()) { iter.next() } could lead to correct behavior
public class StreamSampleIterator<E> implements HgKvIterator<E> {

    private final HgKvIterator<E> iterator;
    private final double sampleFactor;

    private final Random random = new Random();

    private E current;

    public StreamSampleIterator(HgKvIterator<E> iterator, double sampleFactor) {
        this.iterator = iterator;
        this.sampleFactor = sampleFactor;
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
        while (iterator.hasNext()) {
            current = iterator.next();
            if (random.nextDouble() < sampleFactor) {
                return true;
            }
        }

        return false;
    }

    @Override
    public E next() {
        return current;
    }

}
