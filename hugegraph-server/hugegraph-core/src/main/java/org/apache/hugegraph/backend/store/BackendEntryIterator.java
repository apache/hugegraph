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

import java.util.NoSuchElementException;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.exception.LimitExceedException;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public abstract class BackendEntryIterator implements CIter<BackendEntry> {

    private static final Logger LOG = Log.logger(BackendEntryIterator.class);
    public static final long INLINE_BATCH_SIZE = Query.COMMIT_BATCH;

    protected final Query query;

    protected BackendEntry current;
    private long count;

    public BackendEntryIterator(Query query) {
        E.checkNotNull(query, "query");
        this.query = query;
        this.count = 0L;
        this.current = null;
    }

    @Override
    public boolean hasNext() {
        if (this.reachLimit()) {
            return false;
        }

        if (this.current != null) {
            return true;
        }

        return this.fetch();
    }

    @Override
    public BackendEntry next() {
        // Stop if reach capacity
        this.checkCapacity();

        // Stop if reach limit
        if (this.reachLimit()) {
            throw new NoSuchElementException();
        }

        if (this.current == null) {
            this.fetch();
        }

        BackendEntry current = this.current;
        if (current == null) {
            throw new NoSuchElementException();
        }

        this.current = null;
        this.count += this.sizeOf(current);
        return current;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if (PageInfo.PAGE.equals(meta)) {
            return this.pageState();
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }

    public static final void checkInterrupted() {
        if (Thread.interrupted()) {
            throw new BackendException("Interrupted, maybe it is timed out or uses too much memory",
                                       new InterruptedException());
        }
    }

    protected final void checkCapacity() throws LimitExceedException {
        // Stop if reach capacity
        this.query.checkCapacity(this.count());
    }

    protected final boolean reachLimit() {
        /*
         * TODO: if the query is separated with multi sub-queries(like query
         * id in [id1, id2, ...]), then each BackendEntryIterator is only
         * result(s) of one sub-query, so the query offset/limit is inaccurate.
         */

        // Stop if it has reached limit after the previous next()
        return this.reachLimit(this.count);
    }

    protected final boolean reachLimit(long count) {
        try {
            checkInterrupted();
        } catch (Throwable e) {
            try {
                this.close();
            } catch (Throwable ex) {
                LOG.warn("Failed to close backend entry iterator for interrupted query", ex);
            }
            throw e;
        }
        return this.query.reachLimit(count);
    }

    protected final long count() {
        return this.count;
    }

    protected final long fetched() {
        long ccount = this.current == null ? 0 : this.sizeOf(this.current);
        return this.count + ccount;
    }

    protected final void skipPageOffset(String page) {
        PageState pageState = PageState.fromString(page);
        int pageOffset = pageState.offset();
        if (pageOffset > 0) {
            /*
             * Don't update this.count even if skipped page offset,
             * because the skipped records belongs to the last page.
             */
            this.skipOffset(pageOffset);
        }
    }

    protected void skipOffset() {
        long offset = this.query.offset() - this.query.actualOffset();
        if (offset <= 0L) {
            return;
        }
        long skipped = this.skipOffset(offset);
        this.count += skipped;
        this.query.goOffset(skipped);
    }

    protected long skipOffset(long offset) {
        assert offset >= 0L;
        long skipped = 0L;
        // Skip offset
        while (skipped < offset && this.fetch()) {
            assert this.current != null;
            final long size = this.sizeOf(this.current);
            skipped += size;
            if (skipped > offset) {
                // Skip part of sub-items in an entry
                final long skip = size - (skipped - offset);
                skipped -= this.skip(this.current, skip);
                assert skipped == offset;
            } else {
                // Skip entry
                this.current = null;
            }
        }
        return skipped;
    }

    protected long sizeOf(BackendEntry entry) {
        return 1;
    }

    protected long skip(BackendEntry entry, long skip) {
        assert this.sizeOf(entry) == 1;
        // Return the remained sub-items(items)
        return this.sizeOf(entry);
    }

    protected abstract boolean fetch();

    protected abstract PageState pageState();

    public static final class EmptyIterator extends BackendEntryIterator {

        public EmptyIterator(Query query) {
            super(query);
        }

        @Override
        protected boolean fetch() {
            return false;
        }

        @Override
        protected PageState pageState() {
            return PageState.EMPTY;
        }

        @Override
        public void close() throws Exception {
        }
    }
}
