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
import org.apache.hugegraph.backend.query.QueryBatch.BatchIterator;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.ListIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.type.Idfiable;

/** A lazy stream of query batches. Only the final consumer flattens the stream. */
public class QueryResults<R> {

    private final BatchIterator<QueryBatch<R>> batches;
    private final List<Query> queries;
    private final Object metadata;
    private Iterator<R> results;

    public QueryResults(Iterator<R> results, Query query) {
        this(results, new QueryResultContext(query));
    }

    public QueryResults(Iterator<R> results, QueryResultContext context) {
        QueryBatch<R> batch = new QueryBatch<>(results, context);
        this.queries = new ArrayList<>(Collections.singletonList(context.queries().get(0)));
        this.batches = this.trackBatches(new BatchIterator<QueryBatch<R>>() {
            private boolean fetched;

            @Override
            protected QueryBatch<R> fetch() {
                if (this.fetched) {
                    return null;
                }
                this.fetched = true;
                return batch;
            }

            @Override
            protected void closeResources() throws Exception {
                batch.close();
            }
        });
        this.metadata = batch.results();
    }

    private QueryResults(Iterator<QueryBatch<R>> batches, Object metadata) {
        this.queries = new ArrayList<>();
        this.batches = this.trackBatches(batches);
        this.metadata = metadata;
    }

    private BatchIterator<QueryBatch<R>> trackBatches(Iterator<QueryBatch<R>> origin) {
        return new BatchIterator<QueryBatch<R>>() {
            private QueryBatch<R> active;

            @Override
            protected QueryBatch<R> fetch() throws Exception {
                QueryBatch<R> previous = this.active;
                this.active = null;
                QueryBatch.closeAll(previous);
                if (!origin.hasNext()) {
                    return null;
                }
                this.active = origin.next();
                queries.clear();
                queries.add(this.active.context().queries().get(0));
                return this.active;
            }

            @Override
            protected void closeResources() throws Exception {
                QueryBatch<R> previous = this.active;
                this.active = null;
                queries.clear();
                QueryBatch.closeAll(previous, origin);
            }

            @Override
            public Object metadata(String meta, Object... args) {
                return QueryBatch.metadataOf(origin, meta, args);
            }
        };
    }

    public static <R> QueryResults<R> fromBatches(Iterator<QueryBatch<R>> batches) {
        return new QueryResults<>(batches, batches);
    }

    public Iterator<QueryBatch<R>> batches() {
        return this.batches;
    }

    public Iterator<R> iterator() {
        if (this.results == null) {
            this.results = new CIter<R>() {
                private boolean closed;

                @Override
                public boolean hasNext() {
                    if (this.closed) {
                        return false;
                    }
                    try {
                        while (batches.hasNext()) {
                            // Leave the batch and its prefetched element in the shared cursor.
                            if (batches.peek().results().hasNext()) {
                                return true;
                            }
                            batches.next().close();
                        }
                        this.close();
                        return false;
                    } catch (Throwable failure) {
                        QueryResults.close(this, failure);
                        throw QueryBatch.propagate(failure);
                    }
                }

                @Override
                public R next() {
                    if (!this.hasNext()) {
                        throw new NoSuchElementException();
                    }
                    try {
                        return batches.peek().results().next();
                    } catch (Throwable failure) {
                        QueryResults.close(this, failure);
                        throw QueryBatch.propagate(failure);
                    }
                }

                @Override
                public void close() throws Exception {
                    if (!this.closed) {
                        this.closed = true;
                        batches.close();
                    }
                }

                @Override
                public Object metadata(String meta, Object... args) {
                    return QueryBatch.metadataOf(metadata, meta, args);
                }
            };
        }
        return this.results;
    }

    public <T> QueryResults<T> mapBatches(Function<QueryBatch<R>, QueryBatch<T>> mapper) {
        Iterator<QueryBatch<R>> origin = this.batches;
        return new QueryResults<>(new BatchIterator<QueryBatch<T>>() {
            private QueryBatch<?> active;

            @Override
            protected QueryBatch<T> fetch() throws Exception {
                QueryBatch.closeAll(this.active);
                this.active = null;
                if (!origin.hasNext()) {
                    return null;
                }
                QueryBatch<R> batch = origin.next();
                this.active = batch;
                QueryBatch<T> mapped = mapper.apply(batch);
                this.active = mapped;
                return mapped;
            }

            @Override
            protected void closeResources() throws Exception {
                QueryBatch.closeAll(this.active, origin);
            }
        }, this.metadata);
    }

    public <T> QueryResults<T> map(Function<R, T> mapper) {
        return this.mapBatches(batch -> batch.map(mapper));
    }

    public <T> QueryResults<T> flatMap(Function<R, Iterator<T>> mapper) {
        return this.mapBatches(batch -> batch.flatMap(mapper));
    }

    public QueryResults<R> filter(BiPredicate<QueryResultContext, R> predicate) {
        return this.mapBatches(batch -> batch.filter(predicate));
    }

    public <T extends Idfiable> QueryResults<T> keepInputOrderIfNeeded() {
        return this.mapBatches(QueryBatch::keepInputOrder);
    }

    public R one() {
        return one(this.iterator());
    }

    /**
     * Source query of the current batch. A known single source is available before
     * activation; composed streams start empty. Closing clears the diagnostics.
     */
    public List<Query> queries() {
        return Collections.unmodifiableList(this.queries);
    }

    public QueryResults<R> toList() {
        List<QueryBatch<R>> fetched = new ArrayList<>();
        long count = 0L;
        Throwable failure = null;
        try {
            while (this.batches.hasNext()) {
                QueryBatch<R> batch = this.batches.next();
                ListIterator<R> values = toList(batch.results());
                count += values.list().size();
                Query.checkForceCapacity(count);
                fetched.add(new QueryBatch<>(values, batch.context()));
                Query.checkForceCapacity(fetched.size());
            }
        } catch (Throwable e) {
            failure = e;
            throw QueryBatch.propagate(e);
        } finally {
            close(this.batches, failure);
        }
        QueryResults<R> result = new QueryResults<>(fetched.iterator(), this.metadata);
        if (!fetched.isEmpty()) {
            result.queries.add(fetched.get(0).context().queries().get(0));
        }
        return result;
    }

    public static <T, R> QueryResults<R> flatMap(
            Iterator<T> inputs, Function<T, QueryResults<R>> mapper) {
        return fromBatches(new BatchIterator<QueryBatch<R>>() {
            private QueryResults<R> child;

            @Override
            protected QueryBatch<R> fetch() throws Exception {
                while (true) {
                    if (this.child != null && this.child.batches.hasNext()) {
                        return this.child.batches.next();
                    }
                    QueryResults<R> previous = this.child;
                    this.child = null;
                    QueryBatch.closeAll(previous == null ? null : previous.batches);
                    if (!inputs.hasNext()) {
                        return null;
                    }
                    this.child = mapper.apply(inputs.next());
                }
            }

            @Override
            protected void closeResources() throws Exception {
                QueryBatch.closeAll(this.child == null ? null : this.child.batches, inputs);
            }
        });
    }

    @Watched
    public static <T> ListIterator<T> toList(Iterator<T> iterator) {
        Throwable failure = null;
        try {
            return new ListIterator<>(Query.DEFAULT_CAPACITY, iterator);
        } catch (Throwable e) {
            failure = e;
            throw QueryBatch.propagate(e);
        } finally {
            close(iterator, failure);
        }
    }

    @Watched
    public static <T> void fillList(Iterator<T> iterator, List<T> list) {
        Throwable failure = null;
        try {
            while (iterator.hasNext()) {
                list.add(iterator.next());
                Query.checkForceCapacity(list.size());
            }
        } catch (Throwable e) {
            failure = e;
            throw QueryBatch.propagate(e);
        } finally {
            close(iterator, failure);
        }
    }

    @Watched
    public static <T extends Idfiable> void fillMap(Iterator<T> iterator, Map<Id, T> map) {
        Throwable failure = null;
        try {
            while (iterator.hasNext()) {
                T value = iterator.next();
                map.put(value.id(), value);
                Query.checkForceCapacity(map.size());
            }
        } catch (Throwable e) {
            failure = e;
            throw QueryBatch.propagate(e);
        } finally {
            close(iterator, failure);
        }
    }

    @Watched
    public static <T> T one(Iterator<T> iterator) {
        Throwable failure = null;
        try {
            if (iterator.hasNext()) {
                T value = iterator.next();
                if (iterator.hasNext()) {
                    throw new HugeException("Expect just one result, but got at least two: [%s, %s]",
                                            value, iterator.next());
                }
                return value;
            }
            return null;
        } catch (Throwable e) {
            failure = e;
            throw QueryBatch.propagate(e);
        } finally {
            close(iterator, failure);
        }
    }

    private static void close(Object iterator, Throwable failure) {
        try {
            QueryBatch.closeAll(iterator);
        } catch (Throwable closing) {
            if (failure == null) {
                throw QueryBatch.propagate(closing);
            }
            if (closing != failure) {
                failure.addSuppressed(closing);
            }
        }
    }

    public static <T> Iterator<T> iterator(T value) {
        return new OneIterator<>(value);
    }

    public static <T> QueryResults<T> empty() {
        return new QueryResults<T>(QueryResults.<T>emptyIterator(), Query.NONE);
    }

    public static <T> Iterator<T> emptyIterator() {
        return new EmptyIterator<>();
    }

    public interface Fetcher<R> extends Function<Query, QueryResults<R>> {
    }
    private static class EmptyIterator<T> implements CIter<T> {

        @Override
        public Object metadata(String meta, Object... args) {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }

        @Override
        public void close() throws Exception {
            // pass
        }
    }

    private static class OneIterator<T> implements CIter<T> {

        private T element;

        public OneIterator(T element) {
            assert element != null;
            this.element = element;
        }

        @Override
        public Object metadata(String meta, Object... args) {
            return null;
        }

        @Override
        public boolean hasNext() {
            return this.element != null;
        }

        @Override
        public T next() {
            if (this.element == null) {
                throw new NoSuchElementException();
            }
            T result = this.element;
            this.element = null;
            return result;
        }

        @Override
        public void close() throws Exception {
            // pass
        }
    }

}
