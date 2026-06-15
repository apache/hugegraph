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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.Metadatable;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

public abstract class IdHolder {

    protected final Query query;
    protected boolean exhausted;

    public IdHolder(Query query) {
        E.checkNotNull(query, "query");
        this.query = query;
        this.exhausted = false;
    }

    public Query query() {
        return this.query;
    }

    public boolean keepOrder() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s{origin:%s,final:%s}",
                             this.getClass().getSimpleName(),
                             this.query.originQuery(), this.query);
    }

    public abstract boolean paging();

    public abstract Set<Id> all();

    public abstract PageIds fetchNext(String page, long pageSize);

    public static class FixedIdHolder extends IdHolder {

        // Used by Joint Index
        private final Set<Id> ids;

        public FixedIdHolder(Query query, Set<Id> ids) {
            super(query);
            E.checkArgumentNotNull(ids, "The ids can't be null");
            this.ids = ids;
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public Set<Id> all() {
            return this.ids;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("FixedIdHolder.fetchNext");
        }
    }

    public static class PagingIdHolder extends IdHolder {

        private final Function<ConditionQuery, PageIds> fetcher;

        public PagingIdHolder(ConditionQuery query,
                              Function<ConditionQuery, PageIds> fetcher) {
            super(query.copy());
            E.checkArgument(query.paging(),
                            "Query '%s' must include page info", query);
            this.fetcher = fetcher;
        }

        @Override
        public boolean paging() {
            return true;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            if (this.exhausted) {
                return PageIds.EMPTY;
            }

            this.query.page(page);
            this.query.limit(pageSize);

            PageIds result = this.fetcher.apply((ConditionQuery) this.query);
            assert result != null;
            if (result.ids().size() < pageSize || result.page() == null) {
                this.exhausted = true;
            }
            return result;
        }

        @Override
        public Set<Id> all() {
            throw new NotImplementedException("PagingIdHolder.all");
        }
    }

    public static class BatchIdHolder extends IdHolder
            implements CIter<IdHolder> {

        private final Iterator<BackendEntry> entries;
        private final Function<Long, Set<Id>> fetcher;
        private long count;
        private PageIds currentBatch;

        public BatchIdHolder(ConditionQuery query,
                             Iterator<BackendEntry> entries,
                             Function<Long, Set<Id>> fetcher) {
            super(query);
            this.entries = entries;
            this.fetcher = fetcher;
            this.count = 0L;
            this.currentBatch = null;
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public boolean hasNext() {
            if (this.currentBatch != null) {
                return true;
            }
            if (this.exhausted) {
                return false;
            }
            boolean hasNext = this.entries.hasNext();
            if (!hasNext) {
                this.close();
            }
            return hasNext;
        }

        @Override
        public IdHolder next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return this;
        }

        @Override
        public PageIds fetchNext(String page, long batchSize) {
            E.checkArgument(page == null,
                            "Not support page parameter by BatchIdHolder");
            E.checkArgument(batchSize >= 0L,
                            "Invalid batch size value: %s", batchSize);

            if (this.currentBatch != null) {
                return this.getFromCurrentBatch(batchSize);
            }

            if (!this.query.noLimit()) {
                long remaining = this.remaining();
                if (remaining < batchSize) {
                    batchSize = remaining;
                }
            }
            assert batchSize >= 0L : batchSize;
            Set<Id> ids = this.fetcher.apply(batchSize);
            int size = ids.size();
            this.count += size;
            if (size < batchSize || size == 0) {
                this.close();
            }

            // If there is no data, the entries is not a Metadatable object
            if (size == 0) {
                return PageIds.EMPTY;
            } else {
                return new PageIds(ids, PageState.EMPTY);
            }
        }

        @Override
        public Set<Id> all() {
            try {
                Set<Id> ids = this.fetcher.apply(this.remaining());
                if (this.currentBatch != null) {
                    ids.addAll(this.getFromCurrentBatch(Query.NO_LIMIT).ids());
                }
                this.count += ids.size();
                return ids;
            } finally {
                this.close();
            }
        }

        public PageIds peekNext(long size) {
            E.checkArgument(this.currentBatch == null,
                            "Can't call peekNext() twice");
            this.currentBatch = this.fetchNext(null, size);
            return this.currentBatch;
        }

        private PageIds getFromCurrentBatch(long batchSize) {
            assert this.currentBatch != null;
            PageIds result = this.currentBatch;
            this.currentBatch = null;
            return result;
        }

        private long remaining() {
            if (this.query.noLimit()) {
                return Query.NO_LIMIT;
            } else {
                return this.query.total() - this.count;
            }
        }

        @Override
        public void close() {
            if (this.exhausted) {
                return;
            }
            this.exhausted = true;

            CloseableIterator.closeIterator(this.entries);
        }

        @Override
        public Object metadata(String meta, Object... args) {
            E.checkState(this.entries instanceof Metadatable,
                         "Invalid iterator for Metadatable: %s",
                         this.entries.getClass());
            return ((Metadatable) this.entries).metadata(meta, args);
        }
    }
}
