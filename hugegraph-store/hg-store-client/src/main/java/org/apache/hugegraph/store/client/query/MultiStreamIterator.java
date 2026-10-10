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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.store.HgKvIterator;

public class MultiStreamIterator<E> implements HgKvIterator<E> {

    private HgKvIterator<E> currentIterator = null;

    private final List<HgKvIterator<E>> iterators;
    private final Iterator<HgKvIterator<E>> listIterator;
    private final AtomicBoolean closed = new AtomicBoolean();

    public MultiStreamIterator(List<HgKvIterator<E>> iterators) {
        this.iterators = new ArrayList<>(iterators);
        this.listIterator = this.iterators.iterator();
    }

    @Override
    public byte[] key() {
        checkOpen();
        return currentIterator.key();
    }

    @Override
    public byte[] value() {
        checkOpen();
        return currentIterator.value();
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        Set<HgKvIterator<E>> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (HgKvIterator<E> iterator : this.iterators) {
            if (!visited.add(iterator)) {
                continue;
            }
            try {
                iterator.close();
            } catch (RuntimeException | Error error) {
                if (failure == null) {
                    failure = error;
                } else if (failure != error) {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure != null) {
            throw (RuntimeException) failure;
        }
    }

    @Override
    public byte[] position() {
        checkOpen();
        return currentIterator.position();
    }

    @Override
    public void seek(byte[] position) {
        checkOpen();
        this.currentIterator.seek(position);
    }

    private void checkOpen() {
        if (this.closed.get()) {
            throw new IllegalStateException("Iterator is closed");
        }
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
        if (this.closed.get()) {
            return false;
        }
        getNextIterator();
        return currentIterator != null && currentIterator.hasNext();
    }

    @Override
    public E next() {
        if (this.closed.get() || currentIterator == null || !currentIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        return currentIterator.next();
    }
}
