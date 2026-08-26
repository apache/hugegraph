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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.iterator.ListIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.type.Idfiable;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

public class QueryResults<R> {

    private static final Iterator<?> EMPTY_ITERATOR = new EmptyIterator<>();

    private static final QueryResults<?> EMPTY = new QueryResults<>(
            emptyIterator(), Query.NONE);

    private final Iterator<R> results;
    private final List<Query> queries;
    private List<Query> currentQueries;
    private long queryVersion;

    public QueryResults(Iterator<R> results, Query query) {
        this(results);
        this.addQuery(query);
    }

    private QueryResults(Iterator<R> results) {
        this.results = results;
        this.queries = InsertionOrderUtil.newList();
        this.currentQueries = Collections.emptyList();
        this.queryVersion = 0L;
    }

    public void setQuery(Query query) {
        if (!this.queries.isEmpty()) {
            this.queries.clear();
        }
        this.addQuery(query);
    }

    private void addQuery(Query query) {
        E.checkNotNull(query, "query");
        this.addQueries(Collections.singletonList(query));
    }

    private void addQueries(List<Query> queries) {
        assert !queries.isEmpty();
        for (Query query : queries) {
            E.checkNotNull(query, "query");
            this.queries.add(query);
        }
        this.currentQueries = new ArrayList<>(queries);
        this.queryVersion++;
    }

    public Iterator<R> iterator() {
        return this.results;
    }

    public R one() {
        return one(this.results);
    }

    public QueryResults<R> toList() {
        QueryResults<R> fetched = new QueryResults<>(toList(this.results));
        fetched.addQueries(this.queries);
        return fetched;
    }

    public List<Query> queries() {
        return Collections.unmodifiableList(this.queries);
    }

    public <T extends Idfiable> Iterator<T> keepInputOrderIfNeeded(
            Iterator<T> origin) {
        if (!origin.hasNext()) {
            // None result found
            return origin;
        }
        if (!mustSortByInputIds(this.currentQueries)) {
            return origin;
        }
        return new InputOrderIterator<>(this, origin);
    }

    private static boolean mustSortByInputIds(List<Query> queries) {
        assert !queries.isEmpty() : queries;
        for (Query query : queries) {
            if (query instanceof IdQuery &&
                ((IdQuery) query).mustSortByInput()) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unused")
    private boolean bigCapacity() {
        assert !this.queries.isEmpty();
        for (Query query : this.queries) {
            if (query.bigCapacity()) {
                return true;
            }
        }
        return false;
    }

    private static Collection<Id> queryIds(List<Query> queries) {
        assert !queries.isEmpty();
        if (queries.size() == 1) {
            return queries.get(0).ids();
        }

        Set<Id> ids = InsertionOrderUtil.newSet();
        for (Query query : queries) {
            ids.addAll(query.ids());
        }
        return ids;
    }

    @Watched
    public static <T> ListIterator<T> toList(Iterator<T> iterator) {
        try {
            return new ListIterator<>(Query.DEFAULT_CAPACITY, iterator);
        } finally {
            CloseableIterator.closeIterator(iterator);
        }
    }

    @Watched
    public static <T> void fillList(Iterator<T> iterator, List<T> list) {
        try {
            while (iterator.hasNext()) {
                T result = iterator.next();
                list.add(result);
                Query.checkForceCapacity(list.size());
            }
        } finally {
            CloseableIterator.closeIterator(iterator);
        }
    }

    @Watched
    public static <T extends Idfiable> void fillMap(Iterator<T> iterator,
                                                    Map<Id, T> map) {
        try {
            while (iterator.hasNext()) {
                T result = iterator.next();
                assert result.id() != null;
                map.put(result.id(), result);
                Query.checkForceCapacity(map.size());
            }
        } finally {
            CloseableIterator.closeIterator(iterator);
        }
    }

    public static <T, R> QueryResults<R> flatMap(
            Iterator<T> iterator, Function<T, QueryResults<R>> func) {
        @SuppressWarnings("unchecked")
        QueryResults<R>[] qr = new QueryResults[1];
        qr[0] = new QueryResults<>(new FlatMapperIterator<>(iterator, i -> {
            QueryResults<R> results = func.apply(i);
            if (results == null || !results.iterator().hasNext()) {
                return null;
            }
            return new QueryTrackingIterator<>(qr[0], results);
        }));
        return qr[0];
    }

    private long queryVersion() {
        return this.queryVersion;
    }

    private List<Query> currentQueries() {
        return new ArrayList<>(this.currentQueries);
    }

    @Watched
    public static <T> T one(Iterator<T> iterator) {
        try {
            if (iterator.hasNext()) {
                T result = iterator.next();
                if (iterator.hasNext()) {
                    throw new HugeException("Expect just one result, " +
                                            "but got at least two: [%s, %s]",
                                            result, iterator.next());
                }
                return result;
            }
        } finally {
            CloseableIterator.closeIterator(iterator);
        }
        return null;
    }

    public static <T> Iterator<T> iterator(T elem) {
        return new OneIterator<>(elem);
    }

    @SuppressWarnings("unchecked")
    public static <T> QueryResults<T> empty() {
        return (QueryResults<T>) EMPTY;
    }

    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> emptyIterator() {
        return (Iterator<T>) EMPTY_ITERATOR;
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

    private static class QueryTrackingIterator<R>
            extends WrappedIterator<R> {

        private final QueryResults<R> parent;
        private final QueryResults<R> child;
        private long childQueryVersion;

        public QueryTrackingIterator(QueryResults<R> parent,
                                     QueryResults<R> child) {
            this.parent = parent;
            this.child = child;
            this.childQueryVersion = -1L;
        }

        @Override
        protected Iterator<R> originIterator() {
            return this.child.iterator();
        }

        @Override
        protected boolean fetch() {
            Iterator<R> origin = this.child.iterator();
            if (!origin.hasNext()) {
                return false;
            }
            R result = origin.next();
            long queryVersion = this.child.queryVersion();
            if (this.childQueryVersion != queryVersion) {
                this.parent.addQueries(this.child.currentQueries());
                this.childQueryVersion = queryVersion;
            }
            assert this.current == none();
            this.current = result;
            return true;
        }
    }

    private static class InputOrderIterator<T extends Idfiable>
            extends WrappedIterator<T> {

        private final QueryResults<?> queryResults;
        private final Iterator<T> origin;
        private Iterator<T> currentBatch;

        public InputOrderIterator(QueryResults<?> queryResults,
                                  Iterator<T> origin) {
            this.queryResults = queryResults;
            this.origin = origin;
            this.currentBatch = Collections.emptyIterator();
        }

        @Override
        protected Iterator<T> originIterator() {
            return this.origin;
        }

        @Override
        protected boolean fetch() {
            while (true) {
                if (this.currentBatch.hasNext()) {
                    assert this.current == none();
                    this.current = this.currentBatch.next();
                    return true;
                }
                if (!this.origin.hasNext()) {
                    return false;
                }
                this.currentBatch = this.fetchBatch();
            }
        }

        private Iterator<T> fetchBatch() {
            long queryVersion = this.queryResults.queryVersion();
            List<Query> queries = this.queryResults.currentQueries();
            List<T> results = InsertionOrderUtil.newList();
            do {
                results.add(this.origin.next());
                Query.checkForceCapacity(results.size());
            } while (this.origin.hasNext() &&
                     queryVersion == this.queryResults.queryVersion());

            if (!mustSortByInputIds(queries)) {
                return results.iterator();
            }
            Collection<Id> ids = queryIds(queries);
            if (ids.size() <= 1) {
                return results.iterator();
            }

            Map<Id, T> byId = InsertionOrderUtil.newMap();
            for (T result : results) {
                assert result.id() != null;
                byId.put(result.id(), result);
            }
            if (byId.size() > ids.size()) {
                /*
                 * The current query only describes part of this segment.
                 * Preserve backend order because it can't fully define the
                 * order of every returned result.
                 */
                return results.iterator();
            }

            List<T> ordered = new ArrayList<>(results.size());
            for (Id id : ids) {
                T result = byId.remove(id);
                if (result != null) {
                    ordered.add(result);
                }
            }
            ordered.addAll(byId.values());
            return ordered.iterator();
        }
    }
}
