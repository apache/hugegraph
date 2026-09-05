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

package org.apache.hugegraph.backend.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.Metadatable;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.InsertionOrderUtil;

/** A single query's results, with exclusive ownership of its iterator chain. */
public final class QueryBatch<R> implements AutoCloseable {

    private final BatchIterator<R> results;
    private final QueryResultContext context;

    public QueryBatch(Iterator<R> results, QueryResultContext context) {
        this.context = context;
        this.results = new BatchIterator<R>() {
            @Override
            protected R fetch() {
                return results.hasNext() ? results.next() : null;
            }

            @Override
            protected void closeResources() throws Exception {
                closeAll(results);
            }

            @Override
            public Object metadata(String meta, Object... args) {
                return metadataOf(results, meta, args);
            }
        };
    }

    public Iterator<R> results() {
        return this.results;
    }

    public QueryResultContext context() {
        return this.context;
    }

    public <T> QueryBatch<T> map(Function<R, T> mapper) {
        return this.flatMap(value -> {
            T mapped = mapper.apply(value);
            return mapped == null ? Collections.emptyIterator() :
                   Collections.singleton(mapped).iterator();
        });
    }

    public <T> QueryBatch<T> flatMap(Function<R, Iterator<T>> mapper) {
        Iterator<R> origin = this.results;
        return new QueryBatch<>(new BatchIterator<T>() {
            private Iterator<T> child;

            @Override
            protected T fetch() throws Exception {
                while (true) {
                    if (this.child != null && this.child.hasNext()) {
                        T value = this.child.next();
                        if (value != null) {
                            return value;
                        }
                        continue;
                    }
                    Iterator<T> previous = this.child;
                    this.child = null;
                    closeAll(previous);
                    if (!origin.hasNext()) {
                        return null;
                    }
                    this.child = mapper.apply(origin.next());
                }
            }

            @Override
            protected void closeResources() throws Exception {
                Iterator<T> previous = this.child;
                this.child = null;
                closeAll(previous, origin);
            }

            @Override
            public Object metadata(String meta, Object... args) {
                return metadataOf(origin, meta, args);
            }
        }, this.context);
    }

    public QueryBatch<R> filter(BiPredicate<QueryResultContext, R> predicate) {
        return this.map(value -> predicate.test(this.context, value) ? value : null);
    }

    @SuppressWarnings("unchecked")
    public <T extends Idfiable> QueryBatch<T> keepInputOrder() {
        if (!this.context.mustSortByInputIds()) {
            return (QueryBatch<T>) this;
        }
        List<T> values = new ArrayList<>();
        QueryResults.fillList((Iterator<T>) this.results, values);
        List<Id> ids = this.context.inputIds();
        if (ids.size() <= 1) {
            return new QueryBatch<>(values.iterator(), this.context);
        }
        Map<Id, T> byId = InsertionOrderUtil.newMap();
        for (T value : values) {
            byId.put(value.id(), value);
            Query.checkForceCapacity(byId.size());
        }
        if (byId.size() > ids.size()) {
            // A partial ID description cannot order every returned element.
            return new QueryBatch<>(values.iterator(), this.context);
        }
        List<T> ordered = new ArrayList<>(values.size());
        for (Id id : ids) {
            T value = byId.remove(id);
            if (value != null) {
                ordered.add(value);
            }
        }
        ordered.addAll(byId.values());
        return new QueryBatch<>(ordered.iterator(), this.context);
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    /** Internal iterators use null as exhaustion, like the existing mappers. */
    public abstract static class BatchIterator<T> implements CIter<T> {

        private T current;
        private boolean closed;

        protected abstract T fetch() throws Exception;

        protected abstract void closeResources() throws Exception;

        final T peek() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return this.current;
        }

        @Override
        public final boolean hasNext() {
            if (this.closed) {
                return false;
            }
            if (this.current != null) {
                return true;
            }
            try {
                this.current = this.fetch();
                if (this.current == null) {
                    this.close();
                    return false;
                }
                return true;
            } catch (Throwable failure) {
                try {
                    this.close();
                } catch (Throwable closing) {
                    if (closing != failure) {
                        failure.addSuppressed(closing);
                    }
                }
                throw propagate(failure);
            }
        }

        @Override
        public final T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            T value = this.current;
            this.current = null;
            return value;
        }

        @Override
        public final void close() throws Exception {
            if (!this.closed) {
                this.closed = true;
                this.current = null;
                this.closeResources();
            }
        }

        @Override
        public Object metadata(String meta, Object... args) {
            return null;
        }
    }

    public static Object metadataOf(Object iterator, String meta, Object... args) {
        return iterator instanceof Metadatable ?
               ((Metadatable) iterator).metadata(meta, args) : null;
    }

    public static void closeAll(Object... resources) throws Exception {
        Throwable failure = null;
        for (Object resource : resources) {
            if (!(resource instanceof AutoCloseable)) {
                continue;
            }
            try {
                ((AutoCloseable) resource).close();
            } catch (Throwable closing) {
                if (failure == null) {
                    failure = closing;
                } else if (failure != closing) {
                    failure.addSuppressed(closing);
                }
            }
        }
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    public static RuntimeException propagate(Throwable failure) {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        return failure instanceof RuntimeException ? (RuntimeException) failure :
               new HugeException("Failed to iterate query results", failure);
    }
}
