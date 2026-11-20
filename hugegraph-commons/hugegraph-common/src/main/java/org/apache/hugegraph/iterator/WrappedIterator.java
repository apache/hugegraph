/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class WrappedIterator<R> implements CIter<R> {

    private static final Object NONE = new Object();

    protected R current;

    public WrappedIterator() {
        this.current = none();
    }

    @Override
    public boolean hasNext() {
        if (this.current != none()) {
            return true;
        }
        return this.fetch();
    }

    @Override
    public R next() {
        if (this.current == none()) {
            this.fetch();
            if (this.current == none()) {
                throw new NoSuchElementException();
            }
        }
        R current = this.current;
        this.current = none();
        return current;
    }

    @Override
    public void remove() {
        Iterator<?> iterator = this.originIterator();
        if (iterator == null) {
            throw new NoSuchElementException(
                      "The origin iterator can't be null for removing");
        }
        iterator.remove();
    }

    @Override
    public void close() throws Exception {
        Iterator<?> iterator = this.originIterator();
        if (iterator instanceof AutoCloseable) {
            ((AutoCloseable) iterator).close();
        }
    }

    @Override
    public Object metadata(String meta, Object... args) {
        Iterator<?> iterator = this.originIterator();
        if (iterator instanceof Metadatable) {
            return ((Metadatable) iterator).metadata(meta, args);
        }
        throw new IllegalStateException("Original iterator is not Metadatable");
    }

    @SuppressWarnings("unchecked")
    protected static final <R> R none() {
        return (R) NONE;
    }

    public static void close(Iterator<?> iterator) {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to close iterator", e);
            }
        }
    }

    protected abstract Iterator<?> originIterator();

    protected abstract boolean fetch();
}
