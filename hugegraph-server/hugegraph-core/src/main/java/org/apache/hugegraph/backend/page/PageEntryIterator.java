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

import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryBatch.BatchIterator;
import org.apache.hugegraph.backend.query.QueryBatch;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.util.E;

/** Produces pages without probing the following page to delimit the current one. */
public class PageEntryIterator<R> extends BatchIterator<QueryBatch<R>> {

    private final QueryList<R> queries;
    private final long pageSize;
    private final PageInfo pageInfo;
    private Iterator<QueryBatch<R>> pageBatches;
    private long remaining;

    public PageEntryIterator(QueryList<R> queries, long pageSize) {
        this.queries = queries;
        this.pageSize = pageSize;
        this.pageInfo = PageInfo.fromString(queries.parent().pageWithoutCheck());
        E.checkState(this.pageInfo.offset() < queries.total(),
                     "Invalid page offset '%s' exceeds the size of IdHolderList",
                     this.pageInfo.offset());
        this.remaining = queries.parent().limit();
    }

    @Override
    protected QueryBatch<R> fetch() throws Exception {
        while (true) {
            if (this.pageBatches != null && this.pageBatches.hasNext()) {
                return this.pageBatches.next();
            }
            Iterator<QueryBatch<R>> previous = this.pageBatches;
            this.pageBatches = null;
            QueryBatch.closeAll(previous);
            if ((this.remaining != Query.NO_LIMIT && this.remaining <= 0L) ||
                this.pageInfo.offset() >= this.queries.total()) {
                return null;
            }
            long size = this.remaining == Query.NO_LIMIT ? this.pageSize :
                        Math.min(this.pageSize, this.remaining);
            QueryList.PageResults<R> page = this.queries.fetchNext(this.pageInfo, size);
            this.pageBatches = page.results().batches();
            if (page.hasNextPage()) {
                this.pageInfo.page(page.page());
            } else {
                this.pageInfo.increase();
            }
            if (this.remaining != Query.NO_LIMIT) {
                this.remaining -= page.total();
            }
        }
    }

    @Override
    protected void closeResources() throws Exception {
        QueryBatch.closeAll(this.pageBatches, this.queries);
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if (PageInfo.PAGE.equals(meta)) {
            return this.pageInfo.offset() >= this.queries.total() ? null : this.pageInfo;
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
