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

package org.apache.hugegraph.backend.serializer;

import java.util.function.BiFunction;

import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry.BackendIterator;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.util.E;

public class BinaryEntryIterator<Elem> extends BackendEntryIterator {

    protected final BackendIterator<Elem> results;
    protected final BiFunction<BackendEntry, Elem, BackendEntry> merger;

    protected BackendEntry next;

    public BinaryEntryIterator(BackendIterator<Elem> results, Query query,
                               BiFunction<BackendEntry, Elem, BackendEntry> m) {
        super(query);

        E.checkNotNull(results, "results");
        E.checkNotNull(m, "merger");

        this.results = results;
        this.merger = m;
        this.next = null;

        if (query.paging()) {
            assert query.offset() == 0L;
            assert PageState.fromString(query.page()).offset() == 0;
            this.skipPageOffset(query.page());
        } else {
            this.skipOffset();
        }
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (this.results.hasNext()) {
            Elem elem = this.results.next();
            if (this.current != null &&
                this.sizeOf(this.current) >= INLINE_BATCH_SIZE) {
                /*
                 * The current entry already holds a full batch: start the
                 * next entry with this record instead of appending it.
                 * results.position() has already advanced to this record and
                 * a page restarts inclusively from position(), so it must
                 * never point at a record that has been emitted (a page
                 * limit ending exactly at a batch boundary would otherwise
                 * re-emit it as the first record of the next page).
                 */
                assert this.next == null;
                this.next = this.merger.apply(null, elem);
                break;
            }
            BackendEntry merged = this.merger.apply(this.current, elem);
            E.checkState(merged != null, "Error when merging entry");
            if (this.current == null) {
                // The first time to read
                this.current = merged;
            } else if (merged != this.current) {
                // New entry
                assert this.next == null;
                this.next = merged;
                break;
            }
            // Else the record belongs to the current entry

            // When limit exceed, stop fetching
            if (this.reachLimit(this.fetched() - 1)) {
                // Need remove last one because fetched limit + 1 records
                this.removeLastRecord();
                this.results.close();
                break;
            }
        }

        return this.current != null;
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        return sizeOfEntry(entry);
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        BinaryBackendEntry e = (BinaryBackendEntry) entry;
        E.checkState(e.columnsSize() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.removeColumn(0);
        }
        return e.columnsSize();
    }

    @Override
    protected PageState pageState() {
        byte[] position = this.results.position();
        if (position == null) {
            position = PageState.EMPTY_BYTES;
        }
        return new PageState(position, 0, (int) this.count());
    }

    private void removeLastRecord() {
        int lastOne = this.current.columnsSize() - 1;
        ((BinaryBackendEntry) this.current).removeColumn(lastOne);
    }

    public static long sizeOfEntry(BackendEntry entry) {
        /*
         * 3 cases:
         *  1) one vertex per entry
         *  2) one edge per column (one entry <==> a vertex),
         *  3) one element id per column (one entry <==> an index)
         */
        if (entry.type().isEdge() || entry.type().isIndex()) {
            return entry.columnsSize();
        }
        return 1L;
    }
}
