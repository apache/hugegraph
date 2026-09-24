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
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

/**
 * Encapsulate an iterator, perform type conversion via a function, and finally send a supplier
 * .get() command
 *
 * @param <F> Original type
 * @param <E> Target type
 */
public class TypeTransIterator<F, E> implements ScanIterator {

    private final Iterator<F> iterator;
    private final Function<F, E> function;
    private String name = "";
    private ScanIterator originalIterator;
    private Supplier<E> additionSupplier;

    /**
     * is used once. return supper. apply and set to true.
     */
    private boolean flag = false;

    private E data;

    public TypeTransIterator(ScanIterator scanIterator, Function<F, E> function) {
        this.originalIterator = scanIterator;
        this.iterator = new Iterator<F>() {
            @Override
            public boolean hasNext() {
                return scanIterator.hasNext();
            }

            @Override
            public F next() {
                return scanIterator.next();
            }
        };
        this.function = function;
    }

    public TypeTransIterator(ScanIterator scanIterator, Function<F, E> function, String name) {
        this(scanIterator, function);
        this.name = name;
    }

    public TypeTransIterator(Iterator<F> iterator, Function<F, E> function) {
        this.iterator = iterator;
        this.function = function;
    }

    public TypeTransIterator(Iterator<F> iterator, Function<F, E> function, Supplier<E> supplier) {
        this.iterator = iterator;
        this.function = function;
        this.additionSupplier = supplier;
    }

    @Override
    public boolean hasNext() {
        if (this.data != null) {
            return true;
        }

        while (this.iterator.hasNext()) {
            var n = this.iterator.next();
            if (n != null && (data = this.function.apply(n)) != null) {
                return true;
            }
        }

        // look up for the default supplier
        if (this.additionSupplier != null && !this.flag) {
            data = this.additionSupplier.get();
            this.flag = true;
        }

        return data != null;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public <T> T next() {
        if (this.data == null) {
            throw new NoSuchElementException();
        }
        try {
            return (T) this.data;
        } finally {
            // After taking it out, set data to null
            this.data = null;
        }
    }

    @Override
    public void close() {
        if (this.originalIterator != null) {
            this.originalIterator.close();
        }
    }

    @Override
    public String toString() {
        return "TypeTransIterator{" +
               "name='" + name + '\'' +
               ", function=" + function +
               ", additionSupplier=" + additionSupplier +
               ", flag=" + flag +
               ", iterator=" + (originalIterator == null ? iterator : originalIterator) +
               '}';
    }

    /**
     * to java.util.Iterator
     *
     * @return iterator
     */
    public Iterator<E> toIterator() {
        return new InnerIterator(this);
    }

    private class InnerIterator implements Iterator<E>, ScanIterator {

        private final TypeTransIterator<F, E> iterator;

        public InnerIterator(TypeTransIterator<F, E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public void close() {

        }

        @Override
        public E next() {
            return this.iterator.next();
        }
    }
}
