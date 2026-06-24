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

package org.apache.hugegraph.backend.page;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.page.IdHolder.BatchIdHolder;
import org.apache.hugegraph.backend.page.IdHolder.FixedIdHolder;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQuery.OptimizedType;
import org.apache.hugegraph.backend.query.IdQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

public final class QueryList<R> {

    private final Query parent;
    // The size of each page fetched by the inner page
    private final QueryResults.Fetcher<R> fetcher;
    private final List<FlattenQuery<R>> queries;

    public QueryList(Query parent, QueryResults.Fetcher<R> fetcher) {
        this.parent = parent;
        this.fetcher = fetcher;
        this.queries = new ArrayList<>();
    }

    Query parent() {
        return this.parent;
    }

    private QueryResults.Fetcher<R> fetcher() {
        return this.fetcher;
    }

    public void add(IdHolderList holders, long indexBatchSize) {
        // IdHolderList is results of one index query, the query is flattened
        this.queries.add(new IndexQuery(holders, indexBatchSize));
    }

    public void add(Query query) {
        // TODO: maybe need do dedup(for -> flatten)
        this.queries.add(new OptimizedQuery(query));
    }

    public int total() {
        int total = 0;
        for (FlattenQuery<R> q : this.queries) {
            total += q.total();
        }
        return total;
    }

    public boolean empty() {
        return this.queries.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("Query{root:%s,queries:%s}",
                             this.parent, this.queries);
    }

    public QueryResults<R> fetch(int pageSize) {
        assert !this.queries.isEmpty();
        if (this.parent.paging()) {
            @SuppressWarnings("resource") // closed by QueryResults
            PageEntryIterator<R> iter = new PageEntryIterator<>(this, pageSize);
            /*
             * NOTE: PageEntryIterator query will change every fetch time.
             * TODO: sort results by input ids in each page.
             */
            return iter.results();
        }

        // Fetch all results once
        return QueryResults.flatMap(this.queries.iterator(), FlattenQuery::iterator);
    }

    PageResults<R> fetchNext(PageInfo pageInfo, long pageSize) {
        FlattenQuery<R> query = null;
        int offset = pageInfo.offset();
        int visited = 0;
        // Find the first FlattenQuery not visited
        for (FlattenQuery<R> q : this.queries) {
            if (visited + q.total() > offset) {
                /*
                 * The first FlattenQuery not visited is found
                 * q.total() return holders size if it's IndexQuery else 1
                 */
                query = q;
                break;
            }
            visited += q.total();
        }
        E.checkNotNull(query, "query");
        assert offset >= visited;
        return query.iterator(offset - visited, pageInfo.page(), pageSize);
    }

    /**
     * A container that can generate queries
     */
    private interface FlattenQuery<R> {

        /**
         * For non-paging situation
         *
         * @return BackendEntry iterator
         */
        QueryResults<R> iterator();

        /**
         * For paging situation
         *
         * @param index    position IdHolder(Query)
         * @param page     set query page
         * @param pageSize set query page size
         * @return BackendEntry iterator with page
         */
        PageResults<R> iterator(int index, String page, long pageSize);

        int total();
    }

    /**
     * Generate queries from tx.optimizeQuery()
     */
    private class OptimizedQuery implements FlattenQuery<R> {

        private final Query query;

        public OptimizedQuery(Query query) {
            this.query = query;
        }

        @Override
        public QueryResults<R> iterator() {
            // Iterate all
            return fetcher().apply(this.query);
        }

        @Override
        public PageResults<R> iterator(int index, String page, long pageSize) {
            // Iterate by paging
            assert index == 0;
            Query query = this.query.copy();
            query.page(page);
            // Not set limit to pageSize due to PageEntryIterator.remaining
            if (this.query.noLimit()) {
                query.limit(pageSize);
            }

            QueryResults<R> results = fetcher().apply(query);

            // Must iterate all entries before get the next page state
            QueryResults<R> fetched = results.toList();
            PageState pageState = PageInfo.pageState(results.iterator());

            return new PageResults<>(fetched, pageState);
        }

        @Override
        public int total() {
            return 1;
        }

        @Override
        public String toString() {
            return String.format("OptimizedQuery{%s}", this.query);
        }
    }

    /**
     * Generate queries from tx.indexQuery()
     */
    private class IndexQuery implements FlattenQuery<R> {

        // One IdHolder each sub-query
        private final IdHolderList holders;
        // Fetching ids size each time, default 100
        private final long batchSize;

        public IndexQuery(IdHolderList holders, long batchSize) {
            this.holders = holders;
            this.batchSize = batchSize;
        }

        @Override
        public QueryResults<R> iterator() {
            // Iterate all
            if (this.holders.size() == 1) {
                return this.each(this.holders.get(0));
            }
            return QueryResults.flatMap(this.holders.iterator(), this::each);
        }

        private QueryResults<R> each(IdHolder holder) {
            assert !holder.paging();
            Query bindQuery = holder.query();
            this.updateResultsFilter(bindQuery);
            this.updateOffsetIfNeeded(bindQuery);

            // Iterate by all
            if (holder instanceof FixedIdHolder) {
                // The search or joint index query may come here.
                Set<Id> ids = holder.all();
                ids = bindQuery.skipOffsetIfNeeded(ids);
                if (ids.isEmpty()) {
                    return null;
                }

                /*
                 * Sort by input ids because search index results need to keep
                 * in order by ids weight. In addition all the ids (IdQuery)
                 * can be collected by upper layer.
                 */
                return this.queryByIndexIds(ids, holder.keepOrder());
            }

            // Iterate by batch
            assert holder instanceof BatchIdHolder;
            return QueryResults.flatMap((BatchIdHolder) holder, h -> {
                assert ((BatchIdHolder) holder).hasNext();
                long remaining = bindQuery.remaining();
                assert remaining >= 0L || remaining == Query.NO_LIMIT;
                if (remaining > this.batchSize || remaining == Query.NO_LIMIT) {
                    /*
                     * Avoid too many ids in one time query,
                     * Assume it will get one result by each id
                     */
                    remaining = this.batchSize;
                }
                Set<Id> ids = h.fetchNext(null, remaining).ids();
                ids = bindQuery.skipOffsetIfNeeded(ids);
                if (ids.isEmpty()) {
                    return null;
                }

                return this.queryByIndexIds(ids);
            });
        }

        @Override
        public PageResults<R> iterator(int index, String page, long pageSize) {
            // Iterate by paging
            E.checkArgument(0 <= index && index <= this.holders.size(),
                            "Invalid page index %s", index);
            IdHolder holder = this.holders.get(index);
            Query bindQuery = holder.query();
            this.updateResultsFilter(bindQuery);
            PageIds pageIds = holder.fetchNext(page, pageSize);
            if (pageIds.empty()) {
                return PageResults.emptyIterator();
            }

            QueryResults<R> results = this.queryByIndexIds(pageIds.ids());

            return new PageResults<>(results, pageIds.pageState());
        }

        @Override
        public int total() {
            return this.holders.size();
        }

        @Override
        public String toString() {
            return String.format("IndexQuery{%s}", this.holders);
        }

        private void updateOffsetIfNeeded(Query query) {
            Query parent = parent();
            assert parent instanceof ConditionQuery;
            OptimizedType optimized = ((ConditionQuery) parent).optimized();
            if (optimized == OptimizedType.INDEX_FILTER) {
                return;
            }
            // Others sub-query may update parent offset, so copy to this query
            query.copyOffset(parent);
        }

        private void updateResultsFilter(Query query) {
            while (query != null) {
                if (query instanceof ConditionQuery) {
                    ((ConditionQuery) query).updateResultsFilter();
                    return;
                }
                query = query.originQuery();
            }
        }

        private QueryResults<R> queryByIndexIds(Set<Id> ids) {
            return this.queryByIndexIds(ids, false);
        }

        private QueryResults<R> queryByIndexIds(Set<Id> ids, boolean inOrder) {
            IdQuery query = new IdQuery(parent(), ids);
            query.mustSortByInput(inOrder);
            return fetcher().apply(query);
        }
    }

    public static class PageResults<R> {

        public static final PageResults<?> EMPTY = new PageResults<>(
                QueryResults.empty(),
                PageState.EMPTY);

        private final QueryResults<R> results;
        private final PageState pageState;

        public PageResults(QueryResults<R> results, PageState pageState) {
            this.results = results;
            this.pageState = pageState;
        }

        public Iterator<R> get() {
            return this.results.iterator();
        }

        public boolean hasNextPage() {
            return !Bytes.equals(this.pageState.position(),
                                 PageState.EMPTY_BYTES);
        }

        public Query query() {
            List<Query> queries = this.results.queries();
            E.checkState(queries.size() == 1,
                         "Expect query size 1, but got: %s", queries);
            return queries.get(0);
        }

        public String page() {
            return this.pageState.toString();
        }

        public long total() {
            return this.pageState.total();
        }

        @SuppressWarnings("unchecked")
        public static <R> PageResults<R> emptyIterator() {
            return (PageResults<R>) EMPTY;
        }
    }
}
