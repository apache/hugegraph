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

package org.apache.hugegraph.backend.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

public interface BackendEntry extends Idfiable {

    HugeType type();

    @Override
    Id id();

    Id originId();

    Id subId();

    long ttl();

    int columnsSize();

    Collection<BackendColumn> columns();

    void columns(Collection<BackendColumn> columns);

    void columns(BackendColumn column);

    void merge(BackendEntry other);

    boolean mergeable(BackendEntry other);

    void clear();

    default boolean belongToMe(BackendColumn column) {
        return Bytes.prefixWith(column.name, id().asBytes());
    }

    default boolean olap() {
        return false;
    }

    interface BackendIterator<T> extends Iterator<T>, AutoCloseable {

        @Override
        void close();

        byte[] position();
    }

    interface BackendColumnIterator extends BackendIterator<BackendColumn> {

        static BackendColumnIterator empty() {
            return EMPTY;
        }

        static BackendColumnIterator iterator(BackendColumn element) {
            return new OneColumnIterator(element);
        }

        static BackendColumnIterator wrap(Iterator<BackendColumn> iter) {
            return new BackendColumnIteratorWrapper(iter);
        }

        BackendColumnIterator EMPTY = new EmptyIterator();

        final class EmptyIterator implements BackendColumnIterator {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public BackendColumn next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
                // pass
            }

            @Override
            public byte[] position() {
                return null;
            }
        }

        final class OneColumnIterator implements BackendColumnIterator {

            private BackendColumn element;

            public OneColumnIterator(BackendColumn element) {
                assert element != null;
                this.element = element;
            }

            @Override
            public boolean hasNext() {
                return this.element != null;
            }

            @Override
            public BackendColumn next() {
                if (this.element == null) {
                    throw new NoSuchElementException();
                }
                BackendColumn result = this.element;
                this.element = null;
                return result;
            }

            @Override
            public void close() {
                // pass
            }

            @Override
            public byte[] position() {
                return null;
            }
        }

        final class BackendColumnIteratorWrapper implements BackendColumnIterator {

            private final Iterator<BackendColumn> iter;

            public BackendColumnIteratorWrapper(Iterator<BackendColumn> iter) {
                E.checkNotNull(iter, "iter");
                this.iter = iter;
            }

            @Override
            public boolean hasNext() {
                return this.iter.hasNext();
            }

            @Override
            public BackendColumn next() {
                return this.iter.next();
            }

            @Override
            public void close() {
                WrappedIterator.close(this.iter);
            }

            @Override
            public byte[] position() {
                // No paging position for discrete iterators like queryByIds()
                return null;
            }
        }
    }
}
