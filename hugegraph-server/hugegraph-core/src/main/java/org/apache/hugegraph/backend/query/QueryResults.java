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

    public QueryResults(Iterator<R> results, Query query) {
        this(results);
        this.addQuery(query);
    }

    private QueryResults(Iterator<R> results) {
        this.results = results;
        this.queries = InsertionOrderUtil.newList();
    }

    public void setQuery(Query query) {
        if (!this.queries.isEmpty()) {
            this.queries.clear();
        }
        this.addQuery(query);
    }

    private void addQuery(Query query) {
        E.checkNotNull(query, "query");
        this.queries.add(query);
    }

    private void addQueries(List<Query> queries) {
        assert !queries.isEmpty();
        for (Query query : queries) {
            this.addQuery(query);
        }
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
        Collection<Id> ids;
        if (!this.mustSortByInputIds() || this.paging() ||
            (ids = this.queryIds()).size() <= 1) {
            /*
             * Return the original iterator if it's paging query or if the
             * query input is less than one id, or don't have to do sort.
             * NOTE: queryIds() only return the first batch of index query
             */
            return origin;
        }

        // Fill map with all elements
        Map<Id, T> map = InsertionOrderUtil.newMap();
        QueryResults.fillMap(origin, map);

        if (map.size() > ids.size()) {
            /*
             * This means current query is part of QueryResults. For example,
             * g.V().has('country', 'china').has('city', within('HK', 'BJ'))
             * will be converted to
             * g.V().has('country', 'china').has('city', 'HK') or
             * g.V().has('country', 'china').has('city', 'BJ'),
             * and ids is just first index subquery's id, not all.
             */
            ids = map.keySet();
        }

        return new MapperIterator<>(ids.iterator(), map::get);
    }

    private boolean mustSortByInputIds() {
        assert !this.queries.isEmpty() : this;
        for (Query query : this.queries) {
            if (query instanceof IdQuery &&
                ((IdQuery) query).mustSortByInput()) {
                return true;
            }
        }
        return false;
    }

    private boolean paging() {
        assert !this.queries.isEmpty();
        for (Query query : this.queries) {
            Query origin = query.originQuery();
            if (query.paging() || origin != null && origin.paging()) {
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

    private Collection<Id> queryIds() {
        assert !this.queries.isEmpty();
        if (this.queries.size() == 1) {
            return this.queries.get(0).ids();
        }

        Set<Id> ids = InsertionOrderUtil.newSet();
        for (Query query : this.queries) {
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
            /*
             * NOTE: should call results.iterator().hasNext() before
             * results.queries() to collect sub-query with index query
             */
            qr[0].addQueries(results.queries());
            return results.iterator();
        }));
        return qr[0];
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
}
