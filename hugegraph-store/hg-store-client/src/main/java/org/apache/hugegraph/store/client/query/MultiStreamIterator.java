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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.store.HgKvIterator;

public class MultiStreamIterator<E> implements HgKvIterator<E> {

    private HgKvIterator<E> currentIterator = null;

    private final Iterator<HgKvIterator<E>> listIterator;

    public MultiStreamIterator(List<HgKvIterator<E>> iterators) {
        this.listIterator = iterators.iterator();
    }

    @Override
    public byte[] key() {
        return currentIterator.key();
    }

    @Override
    public byte[] value() {
        return currentIterator.value();
    }

    @Override
    public void close() {
        //Todo is syntax correct?
        if (currentIterator != null && currentIterator.hasNext()) {
            currentIterator.close();
        }
    }

    @Override
    public byte[] position() {
        return currentIterator.position();
    }

    @Override
    public void seek(byte[] position) {
        this.currentIterator.seek(position);
    }

    private void getNextIterator() {
        if (currentIterator != null && currentIterator.hasNext()) {
            return;
        }

        while (listIterator.hasNext()) {
            currentIterator = listIterator.next();
            if (currentIterator.hasNext()) {
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        getNextIterator();
        return currentIterator != null && currentIterator.hasNext();
    }

    @Override
    public E next() {
        if (currentIterator == null || !currentIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        return currentIterator.next();
    }
}
